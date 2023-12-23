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

class ArgHandler:
  """ A class to handle command line arguments

  Methods
  -------
  setup_parser():
    Setup the command line parser
  add_standalone_arguments(parser):
    Add arguments for the standalone mode
  add_server_arguments(parser):
    Add arguments for the server mode
  add_client_arguments(parser):
    Add arguments for the client mode
  parse_args():
    Parse the command line arguments
  verify_args(args):
    Verify the command line arguments
    
  Attributes
  ----------
  parser : ArgumentParser
    The command line parser
  """
  def __init__(self):
      self.parser = argparse.ArgumentParser()
      self.setup_parser()

  def setup_parser(self):
      subparsers = self.parser.add_subparsers(
          dest='command',
          help='Use one of the server, client or standalone modes',
          required=True
      )
      standalone = subparsers.add_parser('standalone',
                                         help='Run in standalone mode')
      self.add_standalone_arguments(standalone)

      server = subparsers.add_parser('server', help='Run in server mode')
      self.add_server_arguments(server)

      client = subparsers.add_parser('client', help='Run in client mode')
      self.add_client_arguments(client)

  def add_standalone_arguments(self, standalone):
    standalone.add_argument(
      "-t", "--threads", nargs='?', const=10, type=int, required=False,
      help="number of threads to be used, defaults to 10"
    )
    standalone.add_argument(
      "-e", "--endpoint", required=True,
      help="http(s) endpoint of the S3 service, including the protocol"
    )
    standalone.add_argument(
      "-k", "--key", required=True, help="specify access key"
    )
    standalone.add_argument(
      "-s", "--secret", required=True, help="specify secret key"
    )
    standalone.add_argument(
      "-b", "--bucket", required=True, help="specify bucket name"
    )
    standalone.add_argument(
      "-p", "--prefix", required=False, help="specify prefix"
    )
    standalone.add_argument(
      "-sz", "--size", required=False,
      help="specify size of the object to be uploaded, use K,M,G suffixes"
    )
    standalone.add_argument(
      "-o", "--operation", required=True,
      help="specify operation: upload or download"
    )
    standalone.add_argument(
      "-n", "--number", required=True,
      help="specify number of objects to be uploaded or downloaded"
    )

  def add_server_arguments(self, server):
    server.add_argument(
      "-i", "--ip", required=False, const="0.0.0.0", type=str, nargs='?',
      help="specify IP address to bind to"
    )
    server.add_argument(
      "-p", "--port", required=False, const="8888", type=str, nargs='?',
      help="specify port to bind to"
    )

  def add_client_arguments(self, client):
    client.add_argument(
      "-s", "--server", required=True,
      help="specify IP address and port of the server"
    )

  def parse_args(self):
    args = self.parser.parse_args()
    self.verify_args(args)
    return args
  
  def verify_args(self, args):
    if args.command == 'standalone':
      if args.threads:
        if int(args.threads) > MAX_THREADS:
          logging.error(f'The number of threads must be maximum {MAX_THREADS}.')
          sys.exit(1)
      else:
        args.threads = 10
      logging.info(f'Using {args.threads} threads.')

      if args.size:
        if args.size[-1] == 'K':
          size = int(args.size[:-1]) * 1024
        elif args.size[-1] == 'M':
          size = int(args.size[:-1]) * 1024 * 1024
        elif args.size[-1] == 'G':
          size = int(args.size[:-1]) * 1024 * 1024 * 1024
        else:
          logging.error(f'Invalid size suffix, use K,M,G.')
          sys.exit(1)
        logging.info(f'Using size {args.size}.')
        args.size = size

      if args.prefix:
        if args.prefix[-1] != '/':
          args.prefix += '/'
        logging.info(f'Using prefix {args.prefix}.')
      else:
        args.prefix = ''
        logging.info(f'Using empty prefix.')

      if args.operation not in ['upload', 'download']:
        logging.error(f'Invalid operation, use upload or download.')
        sys.exit(1)
      logging.info(f'Using {args.operation} operation.')

def worker(que, size, prefix):
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

  args = ArgHandler().parse_args()
  if args.command == 'standalone':
    logging.info('Running in standalone mode.')
    s3 = S3(args.endpoint, args.key, args.secret)
    que = Queue()
    for i in range(int(args.number)):
      que.put('testfile-' + str(i))
    start_time = time.time()
    for i in range(args.threads):
      t = Thread(target=worker, args=(que, args.size, args.prefix))
      t.daemon = True
      t.start()
    que.join()
    end_time = time.time()
    logging.info(f'Total time: {end_time - start_time} seconds.')
    print(f'Total time: {end_time - start_time} seconds.')
