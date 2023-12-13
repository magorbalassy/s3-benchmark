#!/usr/bin/python3
import argparse, boto3, logging, os, shutil, sys
import time

from threading import Thread
from queue import Queue

MAX_THREADS = 100

class S3:
  """ A class to represent some S3 operations
  
  Methods
  -------
  list_bucket_objects(bucket):
    List all objects in a bucket's root
    
  put_object(bucket, local_file, remote_file, prefix):
    Upload an object to a bucket
    
  delete_object(bucket, remote_object):
    Delete an object from a bucket
  """
  def __init__(self, endpoint, access_key, secret):
    self.resource = boto3.resource(
      's3',
      endpoint_url=endpoint,
      aws_access_key_id=access_key,
      aws_secret_access_key=secret
      )

  def list_bucket_objects(self, bucket):
    """ Method to list all objects in a bucket's root

    Args:
        bucket (string): the name of the bucket

    Returns:
        array: an array of strings with the names of the objects in the bucket
    """
    s3_bucket = self.resource.Bucket(bucket)
    items = s3_bucket.objects.all()
    result = []
    for item in items:
        result.append(item.key)
    return result

  def put_object(self, bucket, local_file, remote_file, prefix):
    """ Method to upload an object to a bucket

    Args:
        bucket (string): the name of the bucket
        local_file (b'bytes'|file): the object to be uploaded, can be a file or
          bytes
        remote_file (string): the name of the object in the S3 bucket
        prefix (string): the prefix to be used for the object

    Returns:
        json: the JSON response from the S3 API
    """
    return self.resource.Object(bucket, str(prefix + remote_file)).put(Body=local_file)

  def delete_object(self, bucket, remote_object):
    """ Method to delete an object from a bucket

    Args:
        bucket (string): the name of the bucket
        remote_object (string): the name of the object in the S3 bucket

    Returns:
        json: the JSON response from the S3 API
    """
    res = self.resource.Object(bucket, remote_object).delete()
    return res

def random_file(size):
  """ Generate a random file of the specified size

  Args:
      size (number): an integer representing the size of the file in bytes

  Returns:
      string: random bytes of the specified size
  """
  return os.urandom(size)

def worker(que):
  while not que.empty():
    obj = que.get()
    if args.operation == 'upload':
      s3.put_object(args.bucket, random_file(size), obj, prefix)
    elif args.operation == 'download':
      s3.download(args.bucket, obj)
    que.task_done()


if __name__ == "__main__":
  logging.basicConfig(filename='s3-benchmark.log', level=logging.WARNING,
                      format='%(asctime)s - %(levelname)s - %(message)s',
                      datefmt='%m/%d/%Y %I:%M:%S %p')
  logging.info('==== Starting new run ====')

  parser = argparse.ArgumentParser()
  parser.add_argument("-t", "--threads", nargs='?', const=10, type=int, required=False,
    help="number of threads to be used for the operations, defaults to 10")
  parser.add_argument("-e", "--endpoint", required=True,
    help="http(s) endpoint of the S3 service, including the protocol")
  parser.add_argument("-k", "--key", required=True,
    help="specify access key")
  parser.add_argument("-s", "--secret", required=True,
    help="specify secret key")
  parser.add_argument("-b", "--bucket", required=True,
    help="specify bucket name")
  parser.add_argument("-p", "--prefix", required=False,
    help="specify prefix")
  parser.add_argument("-sz", "--size", required=False,
    help="specify size of the object to be uploaded, use K,M,G suffixes")
  parser.add_argument("-o", "--operation", required=True,
    help="specify operation to be performed: upload or download")
  parser.add_argument("-n", "--number", required=True,
    help="specify number of objects to be uploaded or downloaded")
  args = parser.parse_args()
  if args.threads:
    if int(args.threads) > MAX_THREADS:
      logging.error(f'The number of threads must be maximum {MAX_THREADS}.' )
      sys.exit(1)
  else:
    args.threads = 10
  logging.info(f'Using {args.threads} threads.' )
  if args.size:
    if args.size[-1] == 'K':
      size = int(args.size[:-1]) * 1024
    elif args.size[-1] == 'M':
      size = int(args.size[:-1]) * 1024 * 1024
    elif args.size[-1] == 'G':
      size = int(args.size[:-1]) * 1024 * 1024 * 1024
    else:
      logging.error(f'Invalid size suffix, use K,M,G.' )
      sys.exit(1)
    logging.info(f'Using size {args.size}.' )
  else:
    size = 1024
    logging.info(f'Using default size of 1 Kbyte.' )
  if args.prefix:
    if args.prefix[-1] != '/':
      args.prefix += '/'
    prefix = args.prefix
    logging.info(f'Using prefix {prefix}.' )
  else:
    prefix = ''
    logging.info(f'Using empty prefix.' )
  if args.operation == 'upload':
    logging.info(f'Using upload operation.' )
  elif args.operation == 'download':
    logging.info(f'Using download operation.' )
  else:
    logging.error(f'Invalid operation, use upload or download.' )
    sys.exit(1)
  s3 = S3(args.endpoint, args.key, args.secret)
  que = Queue()
  for i in range(int(args.number)):
    que.put('testfile-' + str(i))
  start_time = time.time()
  for i in range(args.threads):
    t = Thread(target=worker, args=(que,))
    t.daemon = True
    t.start()
  que.join()
  end_time = time.time()
  logging.info(f'Total time: {end_time - start_time} seconds.')
  print(f'Total time: {end_time - start_time} seconds.')
