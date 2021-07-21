using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda;
using Amazon.Lambda.Core;
using Amazon.Lambda.Model;
using Amazon.S3;
using Amazon.S3.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace decompress
{
    public class Function
    {
        const int COMPRESSED_BUFFER = 1024 * 1024 * 1;
        const int UNCOMPRESSED_BUFFER = 1024 * 1024 * 1;

        public async Task<string> FunctionHandler(Payload input, ILambdaContext context)
        {
            LambdaLogger.Log($"Payload: {JsonSerializer.Serialize(input)}\r\n");
            long currentPosition = input.initialPosition;
            var s3Client = new AmazonS3Client(RegionEndpoint.USEast1);
            var s3SourceMetadata = await s3Client.GetObjectMetadataAsync(input.sourceBucket, input.sourceFile);
            var totalFileLength = s3SourceMetadata.ContentLength;
            var buffer = new byte[COMPRESSED_BUFFER];

            if (currentPosition == 0 && await S3FileExists(s3Client, input.targetBucket, input.targetFile))
            {
                LambdaLogger.Log($"Deleting object on S3: {input.targetBucket}/{input.targetFile}\r\n");
                await s3Client.DeleteObjectAsync(input.targetBucket, input.targetFile);
            }

            if (currentPosition == 0)
            {
                var multipartResponse = await s3Client.InitiateMultipartUploadAsync(input.targetBucket, input.targetFile);
                LambdaLogger.Log($"Initiated S3 multipart: response: {multipartResponse.HttpStatusCode}:{multipartResponse.UploadId}\r\n");
                input.multipartId = multipartResponse.UploadId;
            }

            while (currentPosition < totalFileLength && !IsTimeoutComing(context))
            {
                var bytesToRead = Math.Min(buffer.Length, totalFileLength - currentPosition);
                LambdaLogger.Log($"Getting chunk from position {currentPosition} with length {bytesToRead}\r\n");
                var s3ObjectSource = await s3Client.GetObjectAsync(new GetObjectRequest()
                {
                    BucketName = input.sourceBucket,
                    Key = input.sourceFile,
                    ByteRange = new ByteRange(currentPosition, currentPosition + bytesToRead)
                });

                LambdaLogger.Log($"Response {s3ObjectSource.HttpStatusCode}\r\n");

                using (var sourceStream = s3ObjectSource.ResponseStream)
                {
                    using (GZipStream gzipStream = new GZipStream(sourceStream, CompressionMode.Decompress))
                    {
                        MemoryStream msOutput = new MemoryStream();
                        await gzipStream.CopyToAsync(msOutput);
                        msOutput.Position = 0;

                        //var uncompressedBuffer = new byte[UNCOMPRESSED_BUFFER];
                        //var read = await gzipStream.ReadAsync(uncompressedBuffer, 0, uncompressedBuffer.Length);
                        var partNumber = ((int)Math.Ceiling((double)(currentPosition / COMPRESSED_BUFFER))) + 1;
                        LambdaLogger.Log($"Uploading part#:{partNumber}, read bytes: {msOutput.Length}\r\n");
                        var uploadResponse = await s3Client.UploadPartAsync(new UploadPartRequest()
                        {
                            UploadId = input.multipartId,
                            PartNumber = partNumber,
                            BucketName = input.targetBucket,
                            Key = input.targetFile,
                            InputStream = msOutput
                            //InputStream = new MemoryStream(uncompressedBuffer, 0, read)
                        });
                        LambdaLogger.Log($"Response {uploadResponse.HttpStatusCode}\r\n");
                    }
                }
                
                currentPosition += bytesToRead;
            }

            if (currentPosition < totalFileLength && IsTimeoutComing(context))
            {
                Payload nextChunkPayload = (Payload)input.Clone();
                nextChunkPayload.initialPosition = currentPosition;
                await NextChunk(nextChunkPayload);
            }
            else
            {
                await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest()
                {
                    BucketName = input.targetBucket,
                    Key = input.targetFile
                });
            }

            LambdaLogger.Log($"Stopped at: {currentPosition}\r\n");
            return $"Stopped at: {currentPosition}\r\n";
        }

        private bool IsTimeoutComing(ILambdaContext context)
        {
            LambdaLogger.Log($"IsTimeoutComing: {context.RemainingTime.TotalSeconds}\r\n");
            return context.RemainingTime.TotalSeconds < 60;
        }

        private async Task<bool> S3FileExists(AmazonS3Client s3Client, string bucket, string fileName)
        {
            LambdaLogger.Log($"Checking existance on S3: {bucket}/{fileName}\r\n");
            return (await s3Client.ListObjectsAsync(bucket, fileName)).S3Objects.Count != 0;
        }

        public async Task NextChunk(Payload input)
        {
            try
            {
                using (AmazonLambdaClient client = new AmazonLambdaClient(RegionEndpoint.USEast1))
                {
                    var request = new InvokeRequest
                    {
                        FunctionName = "decompress2",//Function1:v1 if you use alias
                        Payload = JsonSerializer.Serialize(input),
                        InvocationType = InvocationType.Event
                    };

                    await client.InvokeAsync(request);
                }
            }
            catch (Exception ex)
            {
                LambdaLogger.Log(ex.Message);
            }
        }
    }
}
