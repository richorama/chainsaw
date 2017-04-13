using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Chainsaw
{
	public enum LogState
	{
		Clean,
		Active,
		Full
	}

	public class LogReader : IDisposable
	{
		public LogReader(ISerializer serializer, string directory, string filename, long capacity, LogState initialState)
		{
			this.State = initialState;
			this.Capacity = capacity;
			this.Filename = filename;
			this.File = MemoryMappedFile.CreateFromFile(Path.Combine(directory, filename), FileMode.OpenOrCreate, filename, capacity, MemoryMappedFileAccess.ReadWrite);
			this.serializer = serializer;
		}

		const int headerSize = sizeof(int);
		public LogState State { get; private set; }
		public MemoryMappedFile File { get; }
		public long Capacity { get;  }
		public string Filename { get; }
		readonly ISerializer serializer;

		public void Clean()
		{
			if (this.State == LogState.Clean) return;
			if (this.State == LogState.Active) throw new ApplicationException("you cannot clean the active file");
			this.State = LogState.Clean;
		}

		public IEnumerable<Guid> ReadPositions(int generation, int startingPoint = 0)
		{
			using (var view = this.File.CreateViewAccessor())
			{
				var position = startingPoint;
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

		public T Read<T>(long position, long length)
		{
			using (var stream = this.File.CreateViewStream(position, length))
			{
				return this.serializer.Deserialize<T>(stream);
			}
		}

		public void GoActive()
		{
			if (this.State != LogState.Clean) throw new ApplicationException("the log is not clean");
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

		
		public static LogReader FromString(ISerializer serializer, string value, string directory)
		{
			var parts = value.Split(',');
			return new LogReader(serializer, directory, parts[2], long.Parse(parts[1]), (LogState)int.Parse(parts[0]));
		}
	}
}
