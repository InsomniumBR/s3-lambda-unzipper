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
        const int UNCOMPRESSED_BUFFER = 1024 * 1024 * 10;

        public async Task<string> FunctionHandler(Payload input, ILambdaContext context)
        {
            LambdaLogger.Log($"Payload: {JsonSerializer.Serialize(input)}\r\n");
            var s3Client = new AmazonS3Client(RegionEndpoint.USEast1);
            var s3SourceMetadata = await s3Client.GetObjectMetadataAsync(input.sourceBucket, input.sourceFile);
            var totalBytesRead = 0;

            if (await S3FileExists(s3Client, input.targetBucket, input.targetFile))
            {
                LambdaLogger.Log($"Deleting object on S3: {input.targetBucket}/{input.targetFile}\r\n");
                await s3Client.DeleteObjectAsync(input.targetBucket, input.targetFile);
            }

            var multipartResponse = await s3Client.InitiateMultipartUploadAsync(input.targetBucket, input.targetFile);
            LambdaLogger.Log($"Initiated S3 multipart: response: {multipartResponse.HttpStatusCode}:{multipartResponse.UploadId}\r\n");
            input.multipartId = multipartResponse.UploadId;

            LambdaLogger.Log($"Getting the file {input.sourceFile}\r\n");
            var s3ObjectSource = await s3Client.GetObjectAsync(new GetObjectRequest()
            {
                BucketName = input.sourceBucket,
                Key = input.sourceFile,
            });
            LambdaLogger.Log($"Response {s3ObjectSource.HttpStatusCode}\r\n");

            using (var sourceStream = s3ObjectSource.ResponseStream)
            {
                using (GZipStream gzipStream = new GZipStream(sourceStream, CompressionMode.Decompress))
                {
                    int read = 0;
                    do 
                    {
                        var uncompressedBuffer = new byte[UNCOMPRESSED_BUFFER];
                        read = await gzipStream.ReadAsync(uncompressedBuffer, 0, uncompressedBuffer.Length);
                        totalBytesRead += read;
                        input.partNumber++;
                        LambdaLogger.Log($"Uploading part#:{input.partNumber}, read bytes: {read}, total bytes read: {totalBytesRead}\r\n");
                        var uploadResponse = await s3Client.UploadPartAsync(new UploadPartRequest()
                        {
                            UploadId = input.multipartId,
                            PartNumber = input.partNumber,
                            BucketName = input.targetBucket,
                            Key = input.targetFile,
                            InputStream = new MemoryStream(uncompressedBuffer, 0, read)
                        });
                        //LambdaLogger.Log($"Response {uploadResponse.HttpStatusCode}, total bytes {totalBytesRead}\r\n");
                    }
                    while(read > 0);
                }
            }

            var nextFileName = NextFileName(input.sourceFile) ?? string.Empty;
            if (await S3FileExists(s3Client, input.sourceBucket, nextFileName))
            {
                Payload nextChunkPayload = (Payload)input.Clone();
                nextChunkPayload.sourceFile = nextFileName;
                LambdaLogger.Log($"Starting next file: {nextChunkPayload.sourceFile}\r\n");
                await NextFile(nextChunkPayload);
            }
            else
            {
                await s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest()
                {
                    BucketName = input.targetBucket,
                    Key = input.targetFile
                });
            }

            LambdaLogger.Log($"File processed: {input.sourceFile}, total bytes {totalBytesRead}\r\n");
            return $"File processed: {input.sourceFile}, total bytes {totalBytesRead}\r\n";

        }

        private string NextFileName(string sourceFile)
        {
            var split = sourceFile.Split('.');
            if (int.TryParse(split.LastOrDefault(), out int index)) {
                split[split.Length-1] = (index+1).ToString().PadLeft(split.LastOrDefault().Length, '0');
                return string.Join('.',split);
            }
            return null;
        }

        private async Task<bool> S3FileExists(AmazonS3Client s3Client, string bucket, string fileName)
        {
            LambdaLogger.Log($"Checking existance on S3: {bucket}/{fileName}\r\n");
            return (await s3Client.ListObjectsAsync(bucket, fileName)).S3Objects.Count != 0;
        }

        public async Task NextFile(Payload input)
        {
            try
            {
                using (AmazonLambdaClient client = new AmazonLambdaClient(RegionEndpoint.USEast1))
                {
                    var request = new InvokeRequest
                    {
                        FunctionName = "decompress2",
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
