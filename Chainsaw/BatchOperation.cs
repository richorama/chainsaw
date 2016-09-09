using Chainsaw;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chainsaw
{

    /// <summary>
    /// Not thread safe
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BatchOperation<T>
    {
        public Database<T> Database { get; }

        public List<Record<T>> Operations { get; }

        public BatchOperation(Database<T> database)
        {
            if (null == database) throw new ArgumentNullException(nameof(database));
            this.Database = database;
            this.Operations = new List<Record<T>>();
        }

        public void Set(string key, T value)
        {
            this.Operations.Add(new Record<T>
            {
                Key = key,
                Operation = Operation.Set,
                Value = value
            });
        }

        public void Delete(string key)
        {
            this.Operations.Add(new Record<T>
            {
                Key = key,
                Operation = Operation.Delete
            });
        }

        public void Commit()
        {
            this.Database.Batch(this.Operations.ToArray());
            this.Operations.Clear();
        }

    }
}
