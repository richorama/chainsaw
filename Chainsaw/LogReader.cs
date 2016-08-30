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
            this.File = MemoryMappedFile.CreateFromFile(Path.Combine(directory, filename), FileMode.OpenOrCreate, filename, capacity, MemoryMappedFileAccess.ReadWrite);
        }

        Tuple<long, long>[] positionCache;
        const int headerSize = sizeof(int);
        public LogState State { get; private set; }
        public MemoryMappedFile File { get; }
        public long Capacity { get;  }
        public string Filename { get; }
        readonly Serializer serializer = new Serializer();

        public void Clean()
        {
            lock (this.File)
            {
                if (this.State == LogState.Clean) return;
                if (this.State == LogState.Active) throw new ApplicationException("you cannot clean the active file");

                /*
                using (var view = this.File.CreateViewAccessor())
                {
                    const byte value = 0;
                    for (var i = 0; i < this.Capacity; i++)
                    {
                        var x = view.ReadByte(i);
                        if (x != 0) throw new ApplicationException("not zero");
                        view.Write(i, value);
                    }
                    view.Flush();
                }
                */

                this.State = LogState.Clean;
            }
        }

        public IEnumerable<Guid> ReadPositions(int generation)
        {
            using (var view = this.File.CreateViewAccessor())
            {
                var position = 0;
                while (position < this.Capacity)
                {
                    var length = view.ReadInt32(position);
                    if (length == 0) yield break;

                    yield return LogWriter.GenerateGuid(generation, position + headerSize, length);
                    position += headerSize + length;
                }
            }
        }

	    public int GetNextPosition()
	    {
		    using (var view = this.File.CreateViewAccessor())
			{
				var position = 0;
				while (position < this.Capacity)
				{
					var length = view.ReadInt32(position);
					if (length == 0) return position;

					position += headerSize + length;
				}
				return position;
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
	        this.File?.Dispose();
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
