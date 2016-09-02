using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;

namespace Chainsaw
{
    public enum Operation
    {
        Append,
        Delete
    }


    public struct Record<T>
    {
        public Operation Operation { get; set; }
        public string Key { get; set; }
        public T Value { get; set; }
        public DateTime Time { get; set; }
        public Guid Position { get; set; }
    }

    public class Database<T> : IDisposable
    {
        LogWriter log;
        readonly ConcurrentDictionary<string, Guid> index = new ConcurrentDictionary<string, Guid>();

        public Database(string directory, long logCapacity = 4 * 1024 * 1024)
        {
            log = new LogWriter(directory, logCapacity);
            Guid _;
            foreach (var guid in log.ReadAllKeys())
            {
                var record = log.Read<Record<T>>(guid);
                switch (record.Operation)
                {
                    case Operation.Append:
                        index.AddOrUpdate(record.Key, guid, (__,___) => guid);
                        break;
                    case Operation.Delete:
                        index.TryRemove(record.Key, out _);
                        break;
                }
            }
        }

        public Guid Append(Operation operation, string key, T value)
        {
            var record = new Record<T>
            {
                Operation = operation,
                Key = key,
                Value = value,
                Time = DateTime.UtcNow
            };
            var guid = log.Append(record);
            index.AddOrUpdate(key, guid, (_, __) => guid);
            return guid;
        }

        public T Read(string key)
        {
            Guid result;
            if (index.TryGetValue(key, out result))
            {
                var value = log.Read<Record<T>>(result);
                return value.Value;
            }
            return default(T);
        }


        public IEnumerable<Record<T>> Scan()
        {
            foreach (var key in this.log.ReadAllKeys())
            {
                var value = this.log.Read<Record<T>>(key);
                value.Position = key;
                yield return value;
            }
        }

        public IEnumerable<Record<T>> Scan(Guid from)
        {
            foreach (var key in this.log.ReadAllKeys(from).Skip(1))
            {
                var value = this.log.Read<Record<T>>(key);
                value.Position = key;
                yield return value;
            }
        }


        public void Dispose()
        {
            this.log.Dispose();
        }

    }

}
