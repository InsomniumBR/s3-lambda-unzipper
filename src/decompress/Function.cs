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
        // defines how many bytes to read from s3 each read from stream.
        const int DECOMPRESS_INPUT_BUFFER_SIZE = 1024 * 1024 * 10; 
        
        // impacts on the number of uploaded parts based on extracted bytes.
        const int MAX_TEMP_BUFFER = 1024 * 1024 * 100; 

        private AmazonS3Client _s3Client = null;
        private ILambdaContext _context = null;
        private Payload _payload = null;

        public Function()
        {
            _s3Client = new AmazonS3Client(RegionEndpoint.USEast1);
        }

        public async Task<string> FunctionHandler(Payload payload, ILambdaContext context)
        {
            SetContext(context);
            SetPayload(payload);

            var sourceMetadata = await GetSourceObjectMetadata();
            var totalBytesRead = 0;
            var totalBytesReadPart = 0;

            // adjust inputs
            if (_payload.partList == null) _payload.partList = new List<PartETag>();

            await DeleteObjectIfExists();

            bool firstFileOnJob = (_payload.partNumber == 0);
            if (firstFileOnJob)
            {
                await InitiateMultpartUpload();
                _payload.partNumber = 1;
            }

            MemoryStream tempBuffer = null;
            using (Stream sourceStream = await GetSourceObjectStream())
            {
                using (GZipStream gzipStream = new GZipStream(sourceStream, CompressionMode.Decompress))
                {
                    int bytesRead = 0;
                    do
                    {
                        if (tempBuffer == null) tempBuffer = new MemoryStream(MAX_TEMP_BUFFER);

                        var decompressedBuffer = new byte[DECOMPRESS_INPUT_BUFFER_SIZE];
                        bytesRead = gzipStream.Read(decompressedBuffer, 0, decompressedBuffer.Length);
                        tempBuffer.Write(decompressedBuffer, 0, bytesRead);
                        totalBytesReadPart += bytesRead;
                        //LambdaLogger.Log($"[{RemainingTime()}] just read {bytesRead} bytes\r\n");

                        bool readNothingNow = (bytesRead == 0)
                            , readSomethingBefore = (totalBytesReadPart != 0)
                            , exccededBufferLimit = totalBytesReadPart > MAX_TEMP_BUFFER;

                        if (exccededBufferLimit || (readNothingNow && readSomethingBefore))
                        {
                            LambdaLogger.Log($"[{RemainingTime()}] Uploading part#:{this._payload.partNumber}, total bytes extracted: {totalBytesReadPart}\r\n");
                            await UploadPart(tempBuffer);
                            totalBytesRead += totalBytesReadPart;

                            // reset vars
                            totalBytesReadPart = 0;
                            tempBuffer.Dispose();
                            tempBuffer = null;
                            GC.Collect();
                        }
                    }
                    while (bytesRead > 0);
                }
            }

            var nextFileName = NextFileName(_payload.sourceFile);
            if (nextFileName != null && await S3FileExists(_payload.sourceBucket, nextFileName))
            {
                Payload nextChunkPayload = (Payload)_payload.Clone();
                nextChunkPayload.sourceFile = nextFileName;
                LambdaLogger.Log($"[{RemainingTime()}] Continuing on next file: {nextChunkPayload.sourceFile}\r\n");
                await NextFile(nextChunkPayload);
            }
            else
                await CompleteMultipartUpload();

            LambdaLogger.Log($"[{RemainingTime()}] File processed: {_payload.sourceFile}, output to:{_payload.targetFile}, total bytes {totalBytesRead}\r\n");
            return $"[{RemainingTime()}] File processed: {_payload.sourceFile}, output to:{_payload.targetFile}, total bytes {totalBytesRead}\r\n";

        }

        private async Task<GetObjectMetadataResponse> GetSourceObjectMetadata()
        {
            return await _s3Client.GetObjectMetadataAsync(_payload.sourceBucket, _payload.sourceFile);
        }

        private async Task CompleteMultipartUpload()
        {
            await _s3Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest()
            {
                UploadId = _payload.multipartId,
                BucketName = _payload.targetBucket,
                Key = _payload.targetFile,
                PartETags = _payload.partList
            });
            LambdaLogger.Log($"[{RemainingTime()}] Completing multipart upload with #{_payload.partList.Count} parts, id: {_payload.multipartId}\r\n");
        }

        private async Task UploadPart(MemoryStream tempBuffer)
        {
            tempBuffer.Position = 0;
            var uploadResponse = await _s3Client.UploadPartAsync(new UploadPartRequest()
            {
                UploadId = _payload.multipartId,
                PartNumber = _payload.partNumber,
                BucketName = _payload.targetBucket,
                Key = _payload.targetFile,
                InputStream = tempBuffer
            });
            _payload.partList.Add(new PartETag(uploadResponse.PartNumber, uploadResponse.ETag));
            _payload.partNumber++;
            LambdaLogger.Log($"[{RemainingTime()}] Response {uploadResponse.HttpStatusCode}\r\n");
        }

        private async Task<Stream> GetSourceObjectStream()
        {
            LambdaLogger.Log($"[{RemainingTime()}] Getting the file {_payload.sourceFile}\r\n");
            var s3ObjectSource = await _s3Client.GetObjectAsync(new GetObjectRequest()
            {
                BucketName = _payload.sourceBucket,
                Key = _payload.sourceFile,
            });
            LambdaLogger.Log($"[{RemainingTime()}] Response {s3ObjectSource.HttpStatusCode}\r\n");
            return s3ObjectSource.ResponseStream;
        }

        private async Task InitiateMultpartUpload()
        {
            var multipartResponse = await _s3Client.InitiateMultipartUploadAsync(_payload.targetBucket, _payload.targetFile);
            LambdaLogger.Log($"[{RemainingTime()}] Initiated S3 multipart: response: {multipartResponse.HttpStatusCode}:{multipartResponse.UploadId}\r\n");
            _payload.multipartId = multipartResponse.UploadId;
        }

        private async Task DeleteObjectIfExists()
        {
            if (await S3FileExists(_payload.targetBucket, _payload.targetFile))
            {
                LambdaLogger.Log($"[{RemainingTime()}] Deleting object on S3: {_payload.targetBucket}/{_payload.targetFile}\r\n");
                await _s3Client.DeleteObjectAsync(_payload.targetBucket, _payload.targetFile);
            }
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

        private async Task<bool> S3FileExists(string bucket, string fileName)
        {
            LambdaLogger.Log($"Checking existance on S3: {bucket}/{fileName}\r\n");
            return (await _s3Client.ListObjectsAsync(bucket, fileName)).S3Objects.Count != 0;
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

        private string RemainingTime () {
            return $"Timeout in : {_context.RemainingTime.TotalSeconds}";
        }

        private void SetContext (ILambdaContext context) {
             _context = context;
             LambdaLogger.Log($"[{RemainingTime()}] Starting... \r\n");
        }
        private void SetPayload (Payload payload) { 
            _payload = payload;
            LambdaLogger.Log($"[{RemainingTime()}] Payload: {JsonSerializer.Serialize(_payload)}\r\n");
        }
    }
}
