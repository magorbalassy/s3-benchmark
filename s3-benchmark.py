#!/usr/bin/python3
import argparse, boto3, logging, os, shutil, sys

from threading import Thread
from queue import Queue

MAX_THREADS = 100

class S3 :

  def __init__(self, endpoint, access_key, secret)
    self.resource = boto3.resource(
      's3',
      endpoint_url=endpoint,
      aws_access_key_id=access_key,
      aws_secret_access_key=secret
      )

  del list_bucket_objects(bucket)
    s3_bucket = s3.Bucket(bucket)
    items = s3_bucket.objects.all()
    result = []
    for item in items:
        result.append(item.key)
    return result

  def upload(bucket, local_file, remote_file, prefix)
    s3.Bucket(bucket).upload_file(local_file, remote_file)

  def write_content(bucket, content, remote_object, prefix)
    res = s3.Object(bucket, remote_object).put(Body=content, Prefix=prefix)
    return res

  def delete_object(bucket, remote_object)
    res = s3.Object(bucket, remote_object).delete()
    return res

if __name__ == "__main__":
    logging.basicConfig(filename='s3-benchmark.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('==== Starting new run ====')

    parser = argparse.ArgumentParser()
    parser.add_argument("threads", nargs='?', const=10, type=int,
                    help="number of threads to be used when copying or downloading content")
    parser.add_argument("-e", "--endpoint", required=True,
                    help="http(s) endpoint of the S3 service")
    parser.add_argument("-p", "--protocol", required=True,
                    help="specify protocol")
    args = parser.parse_args()
    if int(args.threads) > MAX_THREADS:
      logging.error(f'The number of threads must be maximum {MAX_THREADS}.' )
      sys.exit(1)
