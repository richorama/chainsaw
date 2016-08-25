using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
