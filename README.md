# S3 benchmarks.

A tool written in Python to perform S3 compatible endpoint benchmarks.  
In order to keep it simple, every functionality is in a sigle file.

# TLDR

Example command:

```
python3 s3-benchmark.py standalone -t 1 -e http://my.minio.local:9000 -k $key -s $secret   -sz=1M -n 20
```

Or the not-abbreviated parameters version:

```
python3 s3-benchmark.py standalone --threads 1 --endpoint http://my.minio.local:9000 --key $key --secret $secret   --size 1M --number 20
```

# Description 

The tool has three operating modes:
- `server`: act as a server for several nodes running in `client` mode, launch the benchmark on the clients, gather and summarize the stats from the clients
- `client`: run in client mode, and send benchmark stats to the server to simulate a cluster doing requests to an S3 endpoint
- `standalone`: run in standalone mode and perform the benchmarks from a single host

At the moment only the standalone mode is implemented.

# Parameters

Each mode has different parameters.

## Server

In server mode we have to specify an optional IP and port to bind to.  
Default IP is 0.0.0.0 (bind to all devices) and default port is 8888.
  
In addition we have to specify all parameters which are required in the standalone mode. Those parameters will be passed on to the connected clients, and the clients will send back the stats to the server.

## Client

In client mode, we have to specify the server to connect to.  
The server will send the required arguments to the client(s) and launch the benchmark in the same time on all clients. The clients will run the benchmark and send the stats to the server.

usage: s3-benchmark.py client [-h] -s SERVER

optional arguments:
-  -h, --help  
   show this help message and exit
-  -s SERVER, --server SERVER  
   specify IP address and port of the server

## Standalone

Usage: 
```
s3-benchmark.py standalone [-h] [-t [THREADS]] -e ENDPOINT -k KEY -s SECRET -b BUCKET [-p PREFIX] [-sz SIZE] -o OPERATION -n NUMBER
```

Arguments in `standalone` mode:
-  -h, --help  
    show this help message and exit
-  -t THREADS, --threads THREADS  
    number of threads to be used for the operations, defaults to 10
-  -e ENDPOINT, --endpoint ENDPOINT  
    http(s) endpoint of the S3 service, including the protocol
-  -k KEY, --key KEY  
    specify access key
-  -s SECRET, --secret SECRET  
    specify secret key
-  -b BUCKET, --bucket BUCKET  
    specify bucket name
-  -p PREFIX, --prefix PREFIX  
    specify prefix
-  -sz SIZE, --size SIZE  
    specify size of the object to be uploaded, use K,M,G suffixes
-  -o OPERATION, --operation OPERATION  
    specify operation to be performed: upload or download
-  -n NUMBER, --number NUMBER  
    specify number of objects to be uploaded or downloaded