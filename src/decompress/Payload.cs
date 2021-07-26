using System;
using System.Collections.Generic;
using Amazon.S3.Model;

public class Payload : ICloneable {
        public string sourceBucket {get; set;}
        public string sourceFile {get; set;}
        public int partNumber {get; set;}
        public string targetBucket {get; set;}
        public string targetFile {get; set;}
        public string multipartId {get; set;}
        public List<PartETag> partList {get; set;}

        public object Clone()
        {
            return new Payload{
                partNumber = this.partNumber,
                multipartId = this.multipartId,
                sourceBucket = this.sourceBucket,
                sourceFile = this.sourceFile,
                targetBucket = this.targetBucket,
                targetFile = this.targetFile,
                partList = this.partList,
            };
        }
    }