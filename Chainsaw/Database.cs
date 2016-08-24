using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

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
    }

    public class Database : IDisposable
    {
        LogWriter log;

        public Database(string directory, long logCapacity = 4 * 1024 * 1024)
        {
            log = new LogWriter(directory, logCapacity);
        }

        public RecordPosition Append<T>(Operation operation, string key, T value)
        {
            var record = new Record<T>
            {
                Operation = operation,
                Key = key,
                Value = value,
                Time = DateTime.UtcNow
            };
            return log.Append(record);
        }


        public void Dispose()
        {
            this.log.Dispose();
        }

    }

}
