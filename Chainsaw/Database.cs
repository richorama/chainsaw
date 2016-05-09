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


    public class Database : IDisposable
    {
        Log log;
        BufferRing buffer;
        const int ENUM_SIZE = sizeof(Operation);
        const int LENGTH_SIZE = sizeof(int);

        public Database(string directory, long logCapacity = 4 * 1024 * 1024)
        {
            log = new Log(directory, logCapacity);
            buffer = new BufferRing();
        }

        public void Append(Operation operation, byte[] key, int keyPosition, int keyLength, byte[] value, int valuePosition, int valueLength)
        {
            var bufferSize = LENGTH_SIZE + ENUM_SIZE + keyLength + valueLength;
            using (var bufferLease = buffer.Allocate(bufferSize))
            {

                /*
                0,1 => key size
                2   => operation
                3-n => key
                n-z => value
                */

                Buffer.BlockCopy(BitConverter.GetBytes(keyLength), 0, bufferLease.Buffer.Buffer, bufferLease.Start, LENGTH_SIZE);
                bufferLease.Buffer.Buffer[bufferLease.Start + LENGTH_SIZE] = (byte)operation;
                Buffer.BlockCopy(key, keyPosition, bufferLease.Buffer.Buffer, bufferLease.Start + LENGTH_SIZE + 1, keyLength);
                Buffer.BlockCopy(value, valuePosition, bufferLease.Buffer.Buffer, bufferLease.Start + LENGTH_SIZE + 1 + keyLength, valueLength);

                log.Append(bufferLease.Buffer.Buffer, bufferLease.Start, bufferSize);
            }
        }


        public void Dispose()
        {
            this.log.Dispose();
        }




    }

}
