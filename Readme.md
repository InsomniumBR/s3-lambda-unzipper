# S3 Lambda Unzipper

The target of this tool is to help you unzip your files from a s3 compressed file to another s3 decompressed file.
For that, you have two tools:
* compress - a small console tool for compressing one large file into various gzip streams
* decompress - an aws lambda that will read the first file, look for the others, and decompress your file back to s3

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
