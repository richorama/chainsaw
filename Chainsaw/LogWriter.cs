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
        public LogReader LogFile { get; set; }
        public int Generation { get; set; }
        public int Index { get; set; }
        public long Position { get; set; }
        public long Length { get; set; }
    }

    public class LogWriter : IDisposable
    {
        long highWaterMark = 0;
        int headerSize = sizeof(int) * 2;
        int generation = 0;
        public int Generation { get { return this.generation; } }
        public List<LogReader> Files { get; private set; }
        public LogReader ActiveFile { get; private set; }
        public long Capacity { get; private set; }
        public string Directory { get; private set; }
        object sync = new object();
        public Action<LogReader, int> HandleFullLog { get; private set; }
        Serializer serializer = new Serializer();
        int nextIndex;
        int firstIndex;

        LogReader AddLogFile()
        {
            var thisLogFilename = $"log.{Guid.NewGuid().ToString()}.log";
            var logFile = new LogReader(this.Directory, thisLogFilename, this.Capacity, LogState.Dirty);
            logFile.Clean();
            this.Files.Add(logFile);
            return logFile;
        }

        IDictionary<int, Tuple<long, long>> cache = new Dictionary<int, Tuple<long, long>>();

        void OpenManifest()
        {
            if (!System.IO.Directory.Exists(this.Directory)) System.IO.Directory.CreateDirectory(this.Directory);

            if (File.Exists(Path.Combine(this.Directory, ".manifest")))
            {
                foreach (var line in File.ReadLines(Path.Combine(this.Directory, ".manifest")).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    this.Files.Add(LogReader.FromString(line, this.Directory));
                }
                this.ActiveFile = this.Files.FirstOrDefault(x => x.State == LogState.Active);
                var highestIndex = 0;
                lowestIndex = int.MaxValue;
                RecordPosition last = new RecordPosition();
                foreach (var position in this.ActiveFile.ReadPositions())
                {
                    highestIndex = Math.Max(position.Index, highestIndex);
                    lowestIndex = Math.Min(position.Index, lowestIndex);
                    last = position;
                    cache.Add(position.Index, new Tuple<long, long>(position.Position, position.Length));
                }
                if (cache.Count > 0)
                {
                    this.highWaterMark = last.Position + last.Length;
                    this.nextIndex = highestIndex + 1;
                    this.firstIndex = lowestIndex;
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
        private int lowestIndex;

        void SaveManifest()
        {
            lock(manifestSync)
            {
                File.WriteAllText(Path.Combine(this.Directory, ".manifest"), string.Join("\r\n", this.Files.Select(x => x.ToString())));
            }
        }

        public LogWriter(string directory, long capacity, Action<LogReader, int> handleFullLog = null)
        {
            this.Files = new List<LogReader>();
            this.Capacity = capacity;
            this.Directory = directory;
            this.HandleFullLog = handleFullLog;

            OpenManifest();
        }

        void RotateLogs()
        {
            this.ActiveFile.GoFull();
            cache.Clear();
            firstIndex = nextIndex + 1;
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

            var index = Interlocked.Increment(ref nextIndex) - 1;
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
                stream.WriteInt32(index);
                stream.WriteInt32((int)lengthStream.Length);
                serializer.Serialize(value, stream);
                stream.Flush();
            }
            cache.Add(index, new Tuple<long, long>(start + headerSize, lengthStream.Length));
            return new RecordPosition
            {
                Position = start + headerSize,
                Length = lengthStream.Length,
                LogFile = this.ActiveFile,
                Index = index,
                Generation = markGeneration
            };
        }


        public T Read<T>(int index)
        {
            // first check to see if it's in the active log

            if (index >= firstIndex)
            {
                Tuple<long, long> item;
                if (cache.TryGetValue(index, out item))
                {
                    return this.ActiveFile.Read<T>(item.Item1, item.Item2);
                }
            }

            foreach (var file in this.Files.Where(x => x.State == LogState.Full))
            {
                if (file.IsIndexInLog(index))
                {
                    return file.Read<T>(index);
                }
            }

            throw new KeyNotFoundException();
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
