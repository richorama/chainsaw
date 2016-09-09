using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using Wire;
using System.IO;

namespace Chainsaw
{
    public enum Operation
    {
        Set,
        Delete
    }


    public struct Record<T>
    {
        public Operation Operation { get; set; }
        public string Key { get; set; }
        public T Value { get; set; }
    }

    

    public class IndexSnapshot
    {
        public KeyValuePair<string,Guid>[] Index { get; set; }
    }
   


    /// <summary>
    /// Thread safe
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Database<T> : IDisposable
    {
        LogWriter log;
        readonly ConcurrentDictionary<string, Guid> index = new ConcurrentDictionary<string, Guid>();
        Serializer serializer = new Serializer();
        string Directory { get; }

        public Database(string directory, long logCapacity = 40 * 1024 * 1024)
        {
            this.log = new LogWriter(directory, logCapacity);
            this.Directory = directory;

            LoadTheIndex();
        }

        public void SnapshotTheIndex()
        {
            var snapshot = new IndexSnapshot
            {
                Index = this.index.AsEnumerable().ToArray(),
            };
            using (var file = File.Create(Path.Combine(this.Directory, "index.index")))
            {
                serializer.Serialize(snapshot, file);
                file.Flush();
            }
        }

        public void LoadTheIndex()
        {
            IEnumerable<Guid> scan = this.log.ReadAllKeys();
            if (File.Exists(Path.Combine(this.Directory, "index.index")))
            {
                var highest = Guid.Empty.ToString();
                using (var file = File.OpenRead(Path.Combine(this.Directory, "index.index")))
                {
                    var snapshot = serializer.Deserialize<IndexSnapshot>(file);
                    foreach (var entry in snapshot.Index)
                    {
                        if (string.Compare(entry.Value.ToString(), highest) > 0) highest = entry.Value.ToString();
                        this.index.AddOrUpdate(entry.Key, entry.Value, (_, __) => entry.Value);
                    }
                }
                if (highest != Guid.Empty.ToString())
                {
                    scan = this.log.ReadAllKeys(Guid.Parse(highest)).Skip(1);
                }
            }

            // catch the index up
            foreach (var position in scan)
            {
                var value = this.log.Read<Record<T>>(position);
                this.index.Apply(value, position);
            }
        }

        public Guid Set(string key, T value)
        {
            var record = new Record<T>
            {
                Operation = Operation.Set,
                Key = key,
                Value = value
            };
            var guid = log.Append(record);
            index.AddOrUpdate(key, guid, (_, __) => guid);
            return guid;
        }

        Guid _;

        public Guid Delete(string key)
        {
            var record = new Record<T>
            {
                Operation = Operation.Delete,
                Key = key,
                Value = default(T)
            };
            var guid = log.Append(record);
            index.TryRemove(key, out _);
            return guid;
        }

        public Guid[] Batch(Record<T>[] records)
        {
            var guids = log.Batch(records);
            var i = 0;
            foreach (var guid in guids)
            {
                var record = records[i++];
                index.Apply(record, guid);
            }
            return guids;
        }

        public T Get(string key)
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
                yield return value;
            }
        }

        public IEnumerable<Record<T>> Scan(Guid from)
        {
            foreach (var key in this.log.ReadAllKeys(from).Skip(1))
            {
                var value = this.log.Read<Record<T>>(key);
                yield return value;
            }
        }

        public int Count => this.index.Keys.Count;


        public BatchOperation<T> CreateBatch()
        {
            return new BatchOperation<T>(this);
        }

        public void Dispose()
        {
            this.log.Dispose();
        }

    }

}
