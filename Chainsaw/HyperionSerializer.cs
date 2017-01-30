using Hyperion;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chainsaw
{
    public class HyperionSerializer : ISerializer
    {
        Serializer serializer;

        public HyperionSerializer()
        {
            this.serializer = new Serializer();
        }

        public T Deserialize<T>(Stream stream)
        {
            return this.serializer.Deserialize<T>(stream);
        }

        public void Serialize(object obj, Stream stream)
        {
            this.serializer.Serialize(obj, stream);
        }
    }
}
