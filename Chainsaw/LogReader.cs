using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Wire;

namespace Chainsaw
{
    public enum LogState
    {
        Dirty,
        Clean,
        Active,
        Full
    }


    public class LogReader : IDisposable
    {
        public LogReader(string directory, string filename, long capacity, LogState initialState)
        {
            this.State = initialState;
            this.Capacity = capacity;
            this.Filename = filename;
            this.File = MemoryMappedFile.CreateFromFile(Path.Combine(directory, filename), FileMode.OpenOrCreate, filename, capacity);
            if (this.State == LogState.Full)
            {
                // the log is full, so figure out the index range
                BuildIndex();
            }
        }

        private void BuildIndex()
        {
            highestIndex = 0;
            lowestIndex = int.MaxValue;
            var hasData = false;
            foreach (var position in this.ReadPositions())
            {
                highestIndex = Math.Max(highestIndex, position.Index);
                lowestIndex = Math.Min(lowestIndex, position.Index);
                hasData = true;
            }
            if (!hasData) return;
            positionCache = new Tuple<long, long>[highestIndex - lowestIndex + 1];
            foreach (var position in this.ReadPositions())
            {
                positionCache[position.Index - lowestIndex] = new Tuple<long, long>(position.Position, position.Length);
            }
        }

        public bool IsIndexInLog(int index)
        {
            return index >= lowestIndex && index <= highestIndex;
        }

        public T Read<T>(int index)
        {
            var arrayPosition = index - lowestIndex;
            var tuple = positionCache[arrayPosition];
            return Read<T>(tuple.Item1, tuple.Item2);
        }

        public object Read(int index)
        {
            var arrayPosition = index - lowestIndex;
            var tuple = positionCache[arrayPosition];
            return Read(tuple.Item1, tuple.Item2);
        }

        Tuple<long, long>[] positionCache;
        int headerSize = sizeof(int) * 2;
        int intSize = sizeof(int);
        public LogState State { get; private set; }
        public MemoryMappedFile File { get; private set; }
        public long Capacity { get; private set; }
        public string Filename { get; private set; }
        Serializer serializer = new Serializer();
        int lowestIndex;
        int highestIndex;

        public void Clean()
        {
            lock (this.File)
            {
                if (this.State == LogState.Clean) return;
                if (this.State == LogState.Active) throw new ApplicationException("you cannot clean the active file");

                using (var view = this.File.CreateViewAccessor())
                {
                    byte value = 0;
                    for (var i = 0; i < this.Capacity; i++)
                    {
                        view.Write(i, value);
                    }
                    view.Flush();
                }

                this.State = LogState.Clean;
            }
        }

        public IEnumerable<RecordPosition> ReadPositions(long start = 0)
        {
            var position = start;
            using (var view = this.File.CreateViewAccessor())
            {
                while (position < this.Capacity)
                {
                    var index = view.ReadInt32(position);
                    var length = view.ReadInt32(position + intSize);
                    if (length == 0) yield break;

                    yield return new RecordPosition
                    {
                        Position = position + headerSize,
                        Length = length,
                        Index = index,
                        LogFile = this
                    };
                    position += headerSize + length;
                }
            }
        }

        public object Read(long position, long length)
        {
            using (var stream = this.File.CreateViewStream(position, length))
            {
                return this.serializer.Deserialize(stream);
            }
        }

        public T Read<T>(long position, long length)
        {
            using (var stream = this.File.CreateViewStream(position, length))
            {
                return this.serializer.Deserialize<T>(stream);
            }
        }

        public void GoActive()
        {
            if (this.State == LogState.Dirty) throw new ApplicationException("the log is not clean");
            this.State = LogState.Active;
        }

        public void GoFull()
        {
            if (this.State != LogState.Active) throw new ApplicationException("logs must be active before they can be full");
            this.State = LogState.Full;
            this.BuildIndex();
        }

        public void Dispose()
        {
            if (this.File != null) this.File.Dispose();
        }

        public override string ToString()
        {
            return $"{(int)this.State},{this.Capacity},{this.Filename}";
        }

        public static LogReader FromString(string value, string directory)
        {
            var parts = value.Split(',');
            return new LogReader(directory, parts[2], long.Parse(parts[1]), (LogState)int.Parse(parts[0]));
        }
    }
}
