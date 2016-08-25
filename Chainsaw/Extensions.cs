using System;

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

    }
}
