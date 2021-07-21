using System;

public class Payload : ICloneable {
        public string sourceBucket {get; set;}
        public string sourceFile {get; set;}
        public long initialPosition {get; set;}
        public string targetBucket {get; set;}
        public string targetFile {get; set;}
        public string multipartId {get; set;}

        public object Clone()
        {
            return new Payload{
                initialPosition = this.initialPosition,
                sourceBucket = this.sourceBucket,
                sourceFile = this.sourceFile,
                targetBucket = this.targetBucket,
                targetFile = this.targetFile
            };
        }
    }