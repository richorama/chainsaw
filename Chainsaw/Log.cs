using System;
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
        public long Position { get; set; }
        public long Length { get; set; }
        public int Generation { get; set; }
        public LogFile LogFile { get; set; }
    }

    public class Log : IDisposable
    {
        long highWaterMark = 0;
        int headerSize = sizeof(int);
        int generation = 0;
        public int Generation { get { return this.generation; } }
        public List<LogFile> Files { get; private set; }
        public LogFile ActiveFile { get; private set; }
        public long Capacity { get; private set; }
        public string Directory { get; private set; }
        object sync = new object();
        public Action<LogFile, int> HandleFullLog { get; private set; }
        Serializer serializer = new Serializer();

        LogFile AddLogFile()
        {
            var thisLogFilename = $"log.{Guid.NewGuid().ToString()}.log";
            var logFile = new LogFile(this.Directory, thisLogFilename, this.Capacity, LogState.Dirty);
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
                    this.Files.Add(LogFile.FromString(line, this.Directory));
                }
                this.ActiveFile = this.Files.FirstOrDefault(x => x.State == LogState.Active);
                var last = this.ActiveFile.ReadPositions().LastOrDefault();
                if (last.Position != 0) //null check not possible
                {
                    this.highWaterMark = last.Position + last.Length;
                }
            }
            else
            {
                // create a new database
                var logFile = this.AddLogFile();
                logFile.GoActive();
                this.ActiveFile = logFile;
                SaveManifest();
            }
        }

        object manifestSync = new object();
        void SaveManifest()
        {
            lock(manifestSync)
            {
                File.WriteAllText(Path.Combine(this.Directory, ".manifest"), string.Join("\r\n", this.Files.Select(x => x.ToString())));
            }
        }

        public Log(string directory, long capacity, Action<LogFile, int> handleFullLog = null)
        {
            this.Files = new List<LogFile>();
            this.Capacity = capacity;
            this.Directory = directory;
            this.HandleFullLog = handleFullLog;

            OpenManifest();
        }

        void RotateLogs()
        {
            this.ActiveFile.GoFull();
            var lastActive = this.ActiveFile;
            
            this.ActiveFile = null;
            var nextGen = Interlocked.Increment(ref generation);

            // send out a notification that the file is ready for compaction
            var thread = new Thread(() =>
            {
                if (null == this.HandleFullLog) return;
                this.HandleFullLog(lastActive, nextGen -1);
                lastActive.Clean();
            });
            thread.Start();

            var nextLog = this.Files.FirstOrDefault(x => x.State == LogState.Clean);
            if (null == nextLog)
            {
                nextLog = AddLogFile();
            }
            nextLog.GoActive();
            this.ActiveFile = nextLog;
            SaveManifest();
        }

        public RecordPosition Append(object value)
        {
            if (null == value) throw new ArgumentNullException(nameof(value));

            var serializer = new Serializer();
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

            var start = mark - length;

            using (var stream = this.ActiveFile.File.CreateViewStream(start, length))
            {
                stream.WriteInt32((int)lengthStream.Length);
                serializer.Serialize(value, stream);
                stream.Flush();
            }
            return new RecordPosition
            {
                Position = start + headerSize,
                Length = lengthStream.Length,
                LogFile = this.ActiveFile,
                Generation = markGeneration
            };
        }

        public void Dispose()
        {
            foreach (var file in this.Files)
            {
                if (file != null) file.Dispose();
            }
        }
    }
}
