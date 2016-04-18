﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading;

namespace Chainsaw
{
    public class RecordPosition
    {
        public long Position { get; set; }
        public int Length { get; set; }
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
        public Action<LogFile> HandleFullLog { get; private set; }

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
                if (null != last)
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

        public Log(string directory, long capacity, Action<LogFile> handleFullLog = null)
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

            // send out a notification that the file is ready for compaction
            var thread = new Thread(() =>
            {
                if (null == this.HandleFullLog) return;
                this.HandleFullLog(lastActive);
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

        public RecordPosition Append(byte[] buffer, int bufferLength = -1)
        {
            if (bufferLength == -1)
            {
                bufferLength = buffer.Length;
            }

            var length = bufferLength + headerSize;
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
                        Interlocked.Increment(ref generation);
                    }
                    mark = Interlocked.Add(ref highWaterMark, length);
                    markGeneration = this.generation;
                }
            }

            var start = mark - length;
            
            using (var view = this.ActiveFile.File.CreateViewAccessor(start, length, MemoryMappedFileAccess.Write))
            {
                view.WriteArray<byte>(headerSize, buffer, 0, bufferLength);
                view.Write(0, bufferLength);
                view.Flush();
            }
            return new RecordPosition
            {
                Position = start + headerSize,
                Length = bufferLength,
                LogFile = this.ActiveFile
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
