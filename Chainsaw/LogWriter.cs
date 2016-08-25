﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading;
using Wire;

namespace Chainsaw
{
    public struct RecordPosition
    {
        public int Generation { get; set; }
        public long Position { get; set; }
        public long Length { get; set; }
    }

    public class LogWriter : IDisposable
    {
        long highWaterMark = 0;
        const int headerSize = sizeof(int);
        int generation = 0;
        public int Generation => this.generation;
        public List<LogReader> Files { get; }
        public LogReader ActiveFile { get; private set; }
        public long Capacity { get; }
        public string Directory { get; }
        readonly object sync = new object();
	    readonly object manifestSync = new object();
        readonly Serializer serializer = new Serializer();

        LogReader AddLogFile(int generation)
        {
            var thisLogFilename = $"log.{generation}.log";
            var logFile = new LogReader(this.Directory, thisLogFilename, this.Capacity, LogState.Dirty);
            logFile.Clean();
            this.Files.Add(logFile);
            return logFile;
        }

        void OpenManifest()
        {
            if (!System.IO.Directory.Exists(this.Directory)) System.IO.Directory.CreateDirectory(this.Directory);

            if (File.Exists(Path.Combine(this.Directory, ".manifest")))
            {
                foreach (var line in File.ReadLines(Path.Combine(this.Directory, ".manifest")).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    this.Files.Add(LogReader.FromString(line, this.Directory));
                    generation++;
                }
                this.ActiveFile = this.Files.FirstOrDefault(x => x.State == LogState.Active);
	            this.highWaterMark = this.ActiveFile.GetNextPosition();
            }
            else
            {
                // create a new database
                var logFile = this.AddLogFile(this.generation);
                logFile.GoActive();
                this.ActiveFile = logFile;
                SaveManifest();
            }
        }


        void SaveManifest()
        {
            lock(manifestSync)
            {
                File.WriteAllText(Path.Combine(this.Directory, ".manifest"), string.Join("\r\n", this.Files.Select(x => x.ToString())));
            }
        }

        public LogWriter(string directory, long capacity)
        {
            this.Files = new List<LogReader>();
            this.Capacity = capacity;
            this.Directory = directory;

            OpenManifest();
        }

        void RotateLogs()
        {
            this.ActiveFile.GoFull();
            var lastActive = this.ActiveFile;
            
            this.ActiveFile = null;
            var nextGen = Interlocked.Increment(ref generation);

            var nextLog = this.Files.FirstOrDefault(x => x.State == LogState.Clean) ?? AddLogFile(nextGen);

	        nextLog.GoActive();

	        this.ActiveFile = nextLog;
            SaveManifest();
	        // TODO: consider creating the next clean log file in the background
        }

        public Guid Append(object value)
        {
            if (null == value) throw new ArgumentNullException(nameof(value));

            var lengthStream = new LengthStream();
            serializer.Serialize(value, lengthStream);

            var length = lengthStream.Length + headerSize;
            var mark = Interlocked.Add(ref highWaterMark, length);
            var markGeneration = this.generation;
            if (mark > this.Capacity)
            {
                // rotate the logs
                lock(sync)
                {
                    if (mark >= this.Capacity && this.generation == markGeneration)
                    {
                        RotateLogs();
                        Interlocked.Exchange(ref highWaterMark, 0);
                    }
                    mark = Interlocked.Add(ref highWaterMark, length);
                    markGeneration = this.generation;
                }
            }


            using (var stream = this.ActiveFile.File.CreateViewStream(mark - length, length))
            {
                stream.WriteByte((byte)lengthStream.Length);
                stream.WriteByte((byte)(lengthStream.Length >> 8));
                stream.WriteByte((byte)(lengthStream.Length >> 16));
                stream.WriteByte((byte)(lengthStream.Length >> 24));
                serializer.Serialize(value, stream);
                stream.Flush();
            }
          
            return GenerateGuid(markGeneration, mark - length + headerSize, lengthStream.Length);
        }

        public static Guid GenerateGuid(long generation, long position, long length)
        {
            return new Guid(new byte[]
            {
                (byte)(generation),
                (byte)(generation >> 8),
                (byte)(generation >> 16),
                (byte)(generation >> 24),
                (byte)(position),
                (byte)(position >> 8),
                (byte)(position >> 16),
                (byte)(position >> 24),
                (byte)(length),
                (byte)(length >> 8),
                (byte)(length >> 16),
                (byte)(length >> 24),
                0,0,0,0
            });
        }
               

        public T Read<T>(Guid record)
        {
            // TODO adds some guards to check the range of args

            var position = record.ParseRecord();
            var log = this.Files[position.Generation];
            return log.Read<T>(position.Position, position.Length);
        }

	    public IEnumerable<Guid> ReadAllKeys()
	    {
		    var generation = 0;
		    foreach (var file in this.Files.Where(x => x.State == LogState.Active || x.State == LogState.Full))
		    {
			    foreach (var record in file.ReadPositions(generation))
			    {
				    yield return record;
			    }
                generation++;
		    }
	    }



	    public void Dispose()
        {
            foreach (var file in this.Files)
            {
	            file?.Dispose();
            }
        }
    }
}
