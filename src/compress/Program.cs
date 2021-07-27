using System;
using System.IO;
using System.IO.Compression;

namespace compress
{
    class Program
    {
        const int MB = 1024 * 1024;

        struct Settings
        {
            internal string input { get; set; }
            internal string output { get; set; }
            internal int size { get; set; }
            public bool IsValid()
            {
                return this.size > 0 && this.input != null && this.output != null;
            }
        }

        static void Main(string[] args)
        {
            if (args.Length != 6) {
                 PrintHelp();
                 return;
            }

            var settings = ParseSettings(args);

            if (!settings.IsValid()) {
                Abend("Invalid arguments detected, check if size is greater than 0.");
                return;
            }
            else
                Compress(settings);
        }

        private static void Compress(Settings settings)
        {
            if (!File.Exists(settings.input))
            {
                Abend($"Input file does not exist: {settings.input}");
                return;
            }

            if (File.Exists(settings.output))
            {
                Abend($"Output file already exist: {settings.output}");
                return;
            }

            int partNumber = 1;
            using (var sourceStream = File.OpenRead(settings.input))
            {
                var partNumberSuffix = partNumber.ToString().PadLeft(3, '0');
                var fileName = $"{settings.output}.{partNumberSuffix}";
                FileStream underlyingOutputStream = File.OpenWrite(fileName);
                GZipStream outputStream = new GZipStream(underlyingOutputStream, CompressionLevel.Optimal);
                var byteRead = 0;
                var buffer = new byte[4096]; // optimized for 4k sectors (ntfs like)

                do
                {
                    byteRead = sourceStream.Read(buffer, 0, buffer.Length);
                    outputStream.Write(buffer, 0, byteRead);

                    if (underlyingOutputStream.Position >= (settings.size * MB))
                    {
                        outputStream.Flush();
                        outputStream.Dispose();
                        Console.WriteLine($"File generated: {fileName}");
                        partNumberSuffix = (++partNumber).ToString().PadLeft(3, '0');
                        fileName = $"{settings.output}.{partNumberSuffix}";
                        underlyingOutputStream = File.OpenWrite(fileName);
                        outputStream = new GZipStream(underlyingOutputStream, CompressionLevel.Optimal);
                    }
                }
                while (byteRead > 0);

                if(outputStream != null) { 
                    outputStream.Flush();
                    outputStream.Dispose();
                    Console.WriteLine($"File generated: {fileName}");
                }
            }
        }

        private static Settings ParseSettings(string[] args)
        {
            Settings s = new Settings();

            for (int i = 0; i < args.Length; i += 2)
            {
                var arg = args[i].ToLower();
                switch (arg)
                {
                    case "-s":
                        s.size = int.Parse(args[i + 1]);
                        break;
                    case "-i":
                        s.input = args[i + 1];
                        break;
                    case "-o":
                        s.output = args[i + 1];
                        break;
                    default:
                        Abend($"Invalid argument {arg}");
                        break;
                }
            }

            return s;
        }

        private static void Abend(string message)
        {
            Console.WriteLine($"ERROR: {message}");
            PrintHelp();
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Gzip Fan-out Compressor");
            Console.WriteLine("Objective: read one input file, generate multiple gzip streams");
            Console.WriteLine("Parameters:");
            Console.WriteLine("    -s    maximum size of each stream in megabytes");
            Console.WriteLine("    -i    input file to be compressed");
            Console.WriteLine("    -o    output generated file");
            Console.WriteLine("Usage: compress -s 100 -i large-file.txt -o compressed.gz");
        }
    }
}
