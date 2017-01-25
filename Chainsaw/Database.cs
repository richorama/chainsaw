using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Wire;

/*  TODO

    Support index saving on a separate thread
    Compaction
    Investigate BTree instead of concurrent dictionary
    Better GUID comparison when loading index
    Split index into a separate class (concern)
    IOC on Serailizer
    Catch exception with record size greater than log size
    Retrieve current value for high water mark
    Readthrough cache?
    Benchmark with some serious data

*/

namespace Chainsaw
{
    public enum Operation
    {
        Set,
        Delete
    }

    public struct Rec<T>
    {
        public Operation Operation { get; set; }
        public string Key { get; set; }
        public T Value { get; set; }
    }

    public struct Record<T>
    {
        public Operation Operation { get; set; }
        public string Key { get; set; }
        public T Value { get; set; }
        public Guid Tag { get; set; }
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
        HashSet<ISecondaryIndex<T>> Indexes = new HashSet<ISecondaryIndex<T>>();

        public Database(string directory = "db", long logCapacity = 40 * 1024 * 1024)
        {
            this.log = new LogWriter(directory, logCapacity);
            this.Directory = directory;

            LoadTheIndex();
        }

        public Func<Y,IEnumerable<T>> RegisterSecondaryIndex<Y>(Func<T, Y> indexer)
        {
            var secondaryIndex = new SecondaryIndex<T, Y>(indexer);

            this.Indexes.Add(secondaryIndex);
            foreach (var item in this.Scan())
            {
                secondaryIndex.Add(item.Key, item.Value, item.Tag);
            }

            return y => 
            {
                return secondaryIndex.Query(y).Select(x => log.Read<Rec<T>>(x).Value);
            };
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
                var value = this.log.Read<Rec<T>>(position);
                this.index.Apply(value, position);
            }
        }

        public Guid Set(string key, T value)
        {
            var record = new Rec<T>
            {
                Operation = Operation.Set,
                Key = key,
                Value = value
            };
            var guid = log.Append(record);
            index.AddOrUpdate(key, guid, (_, __) => guid);

            foreach (var index in this.Indexes)
            {
                index.Add(key, value, guid);
            }

            return guid;
        }

        Guid _;

        public Guid Delete(string key)
        {
            var record = new Rec<T>
            {
                Operation = Operation.Delete,
                Key = key,
                Value = default(T)
            };
            var guid = log.Append(record);
            index.TryRemove(key, out _);

            foreach (var index in this.Indexes)
            {
                index.Remove(key);
            }

            return guid;
        }

        public Guid[] Batch(Rec<T>[] records)
        {
            var guids = log.Batch(records);
            var i = 0;
            foreach (var guid in guids)
            {
                var record = records[i++];
                index.Apply(record, guid);
            }

            i = 0;
            foreach (var guid in guids)
            {
                var record = records[i++];
                if (record.Operation == Operation.Set)
                {
                    foreach (var index in this.Indexes)
                    {
                        index.Add(record.Key, record.Value, guid);
                    }
                }
                else
                {
                    foreach (var index in this.Indexes)
                    {
                        index.Remove(record.Key);
                    }
                }
            }

            return guids;
        }

        public T Get(string key)
        {
            Guid result;
            if (index.TryGetValue(key, out result))
            {
                var value = log.Read<Rec<T>>(result);
                return value.Value;
            }
            return default(T);
        }

        public Guid GetTag(string key)
        {
            Guid result;
            if (index.TryGetValue(key, out result))
            {
                return result;
            }
            return Guid.Empty;
        }

        public IEnumerable<Record<T>> Scan()
        {
            foreach (var key in this.log.ReadAllKeys())
            {
                var value = this.log.Read<Rec<T>>(key);
                yield return new Record<T>
                {
                    Key = value.Key,
                    Operation = value.Operation,
                    Tag = key,
                    Value = value.Value
                };
            }
        }

        public IEnumerable<Record<T>> Scan(Guid from)
        {
            foreach (var key in this.log.ReadAllKeys(from).Skip(1))
            {
                var value = this.log.Read<Rec<T>>(key);
                yield return new Record<T>
                {
                    Key = value.Key,
                    Operation = value.Operation,
                    Tag = key,
                    Value = value.Value
                };
            }
        }

        public int Count => this.index.Keys.Count;


        public BatchOperation<T> CreateBatch()
        {
            return new BatchOperation<T>(this);
        }

        public void Dispose()
        {
            this.log?.Dispose();
        }

    }

}
