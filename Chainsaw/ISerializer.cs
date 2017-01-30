using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chainsaw
{
    public interface ISerializer
    {
        void Serialize(object obj, Stream stream);
        T Deserialize<T>(Stream stream);
    }
}
