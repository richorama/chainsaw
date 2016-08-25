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
        }

        Tuple<long, long>[] positionCache;
        int headerSize = sizeof(int);
        int intSize = sizeof(int);
        public LogState State { get; private set; }
        public MemoryMappedFile File { get; private set; }
        public long Capacity { get; private set; }
        public string Filename { get; private set; }
        Serializer serializer = new Serializer();

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

        public IEnumerable<RecordPosition> ReadPositions(int generation)
        {
            using (var view = this.File.CreateViewAccessor())
            {
                var position = 0;
                while (position < this.Capacity)
                {
                    var length = view.ReadInt32(position);
                    if (length == 0) yield break;

                    yield return new RecordPosition
                    {
                        Position = position + headerSize,
                        Length = length,
                        Generation = generation
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
