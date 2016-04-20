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

        public void Append(Operation operation, byte[] key, byte[] value)
        {
            var bufferSize = LENGTH_SIZE + ENUM_SIZE + key.Length + value.Length;
            using (var bufferLease = buffer.Allocate(bufferSize))
            {

                /*
                0,1 => key size
                2   => operation
                3-n => key
                n-z => value
                */

                Buffer.BlockCopy(BitConverter.GetBytes(key.Length), 0, bufferLease.Buffer.Buffer, bufferLease.Start, LENGTH_SIZE);
                bufferLease.Buffer.Buffer[bufferLease.Start + LENGTH_SIZE] = (byte)operation;
                Buffer.BlockCopy(key, 0, bufferLease.Buffer.Buffer, bufferLease.Start + LENGTH_SIZE + 1, key.Length);
                Buffer.BlockCopy(value, 0, bufferLease.Buffer.Buffer, bufferLease.Start + LENGTH_SIZE + 1 + key.Length, value.Length);

                log.Append(bufferLease.Buffer.Buffer, bufferLease.Start, bufferSize);
            }
        }


        public void Dispose()
        {
            this.log.Dispose();
        }




    }

}
