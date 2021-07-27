# S3 Lambda Unzipper

The target of this tool is to help you unzip your files from a s3 compressed file to another s3 decompressed file.
For that, you have two tools:
* compress - a small console tool for compressing one large file into various gzip streams
* decompress - an aws lambda that will read the first file, look for the others, and decompress your file back to s3

## Can I use my own compression tool?

Unfortunately, no. When you zip your file and ask the 7zip (for instance) to split the file, just the first file has the stream header with information useful on decompression. This compressor is a simple tool to generate N streams, each one with a header and containing a part of the big file being zipped. So, we have many complete streams, instead of a splited stream.

*PS: If you want to decompress just one Gzip file, compressed anywhere (7zip or another), it will work. But if you want to unzip splitted files you must use my compressor.*

## How this lambda works

Since the lambda has only 15 minutes for running to completion, the context being processed inside a lambda cannot surpass this interval to be uncompressed and saved back to s3. This lambda will read the source file from s3, and write the uncompressed file back to s3 on the target file. If this function detects a 'continuation' (the first file ends with .001, the next file .002 exists) it will call itself again (loop) for continuing the job, until the last part found is uncompressed and the multipart upload (created on the first part) is completed.

## Running the compressor

After compiling, run the compressor on the bigfile that you want to upload to AWS zipped, and define the size of each part generated.
If you use lambdas running with 2048MB as per default, my sugestion is not to surpass the size of 2GB per file, but you can test and check how much time 1 file takes to be decompressed and choose the best size for your (always trying to avoid the 15 minutes limit).

```
    Gzip Fan-out Compressor
    Objective: read one input file, generate multiple gzip streams
    Parameters:
        -s    maximum size of each stream in megabytes
        -i    input file to be compressed
        -o    output generated file
    Usage: compress -s 100 -i large-file.txt -o compressed.gz
```

## Executing the lambda function

You can execute this lambda using your preferred trigger, HTTP, or mannually invoking it through the aws cli.
The expected payload on this lambda is:

```
    {
      "sourceBucket": "mys3buckettemp",
      "sourceFile": "myzippedfile.gz.001",
      "targetBucket": "mys3buckettemp",
      "targetFile": "myunzippedfile.log"
    }
```

## Building the compressor
Building the compressor is easy with dotnet tool

```
    cd "s3-lambda-unzipper/src/compress"
    dotnet build
```

## Publishing this lambda at your AWS account:

First, be sure your aws client is installed and configured.

You can deploy your application using the [Amazon.Lambda.Tools Global Tool](https://github.com/aws/aws-extensions-for-dotnet-cli#aws-lambda-amazonlambdatools) from the command line.

Install Amazon.Lambda.Tools Global Tools if not already installed.
```
    dotnet tool install -g Amazon.Lambda.Tools
```

If already installed check if new version is available.
```
    dotnet tool update -g Amazon.Lambda.Tools
```

Deploy function to AWS Lambda
```
    cd "s3-lambda-unzipper/src/decompress"
    dotnet lambda deploy-function
```
