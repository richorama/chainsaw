using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chainsaw
{
    public interface ISecondaryIndex<T>
    {
        void Add(string key, T value, Guid position);
        void Remove(string key);
    }

    public class SecondaryIndex<T,Y> : ISecondaryIndex<T>
    {
        public Func<T, Y> Indexer { get; private set; }

        public ConcurrentDictionary<Y, HashSet<Guid>> Index { get; private set; }

        public ConcurrentDictionary<string, Tuple<Guid,HashSet<Guid>>> KeyIndex { get; private set; }

        public SecondaryIndex(Func<T,Y> indexer)
        {
            this.Indexer = indexer;
            this.Index = new ConcurrentDictionary<Y, HashSet<Guid>>();
            this.KeyIndex = new ConcurrentDictionary<string, Tuple<Guid, HashSet<Guid>>>();
        }

        public IEnumerable<Guid> Query(Y value)
        {
            HashSet<Guid> results = null;
            this.Index.TryGetValue(value, out results);
            return results; 
        }
 
        public void Add(string key, T value, Guid position)
        {
            var y = this.Indexer(value);
       
            var hash = this.Index.AddOrUpdate(y, new HashSet<Guid>(new Guid[] { position }), (_, old) =>
            {
                lock (old)
                {
                    old.Add(position);
                }
                return old;
            });

            this.KeyIndex.AddOrUpdate(key, x => new Tuple<Guid, HashSet<Guid>>(position, hash), (_, __) => new Tuple<Guid, HashSet<Guid>>(position, hash));
            return;
        }

        public void Remove(string key)
        {
            Tuple<Guid,HashSet<Guid>> keyIndex = null;
            if (this.KeyIndex.TryRemove(key, out keyIndex))
            {
                lock (keyIndex.Item2)
                {
                    keyIndex.Item2.Remove(keyIndex.Item1);
                }
            }
        }

    }
}
