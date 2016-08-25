using System;
using System.IO;

namespace Chainsaw
{
    /// <summary>
    /// A stream implementation to measure the length of a serialized object
    /// </summary>
    public class LengthStream : Stream
    {
        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        long length;

        public override long Length => this.length;

	    public override long Position { get; set; }


	    public override void Flush()
        {
            
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            this.length = value;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.length += count;
        }
    }
}
