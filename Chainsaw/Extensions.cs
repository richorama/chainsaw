using System;
using System.Collections.Concurrent;

namespace Chainsaw
{
    public static class Extensions
    {
        public static RecordPosition ParseRecord(this Guid value)
        {
            var bytes = value.ToByteArray();
            return new RecordPosition {
                Generation = BitConverter.ToInt32(bytes, 0),
                Position = BitConverter.ToInt32(bytes,4),
                Length = BitConverter.ToInt32(bytes, 8)
            };
        }
        static Guid _;
        public static void Apply<T>(this ConcurrentDictionary<string, Guid> value, Record<T> record, Guid position)
        {
            switch (record.Operation)
            {
                case Operation.Append:
                    value.AddOrUpdate(record.Key, position, (_, __) => position);
                    break;
                case Operation.Delete:
                    value.TryRemove(record.Key, out _);
                    break;
            }
        }

    }
}
