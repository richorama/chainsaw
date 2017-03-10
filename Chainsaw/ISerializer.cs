using System.IO;

namespace Chainsaw
{
    public interface ISerializer
    {
        void Serialize(object obj, Stream stream);
        T Deserialize<T>(Stream stream);
    }
}
