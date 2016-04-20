using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Chainsaw
{

    public enum BufferState
    {
        Empty,
        Active,
        Full
    }

    public class BufferSession : IDisposable
    {
        public RBuffer Buffer { get; private set; }
        public int Start { get; private set; }
        public int Length { get; private set; }

        public BufferSession(RBuffer buffer, int start, int length)
        {
            this.Buffer = buffer;
            this.Start = start;
            this.Length = length;
            Interlocked.Increment(ref this.Buffer.References);
        }

        public void Dispose()
        {
            Interlocked.Decrement(ref this.Buffer.References);
        }
    }

    public class RBuffer
    {
        public RBuffer(long capacity = 1 * 1024 * 1024)
        {
            this.Buffer = new byte[capacity];
            this.Mark = 0;
            this.State = BufferState.Empty;
        }

        public byte[] Buffer { get; private set; }
        public BufferState State {get;private set;}
        public int Mark;
        public int References;
        public void Reset()
        {
            if (this.References > 0) throw new InvalidOperationException("Buffer is not empty");
            if (this.State == BufferState.Active) throw new InvalidOperationException("Buffer is active");

            lock(this.Buffer)
            {
                this.Mark = 0;
                this.References = 0;
                this.State = BufferState.Empty;
            }
        }
        public void AttemptToEmpty()
        {
            if (this.State == BufferState.Full && this.References == 0)
            {
                this.Mark = 0;
                this.State = BufferState.Empty;
            }
        }
        public void GoActive()
        {
            if (this.Mark != 0) throw new InvalidOperationException("Mark is not empty");
            if (this.References != 0) throw new InvalidOperationException("References is not zero");
            this.State = BufferState.Active;
        }

        public void GoFull()
        {
            if (this.State != BufferState.Active) throw new InvalidOperationException("Mark is not active");
            this.State = BufferState.Full;
        }
    }

    public class BufferRing
    {
        object sync = new object();
        public List<RBuffer> Ring { get; private set; }
        public RBuffer ActiveBuffer { get; private set; }
        public long Capacity { get; private set; }

        public BufferRing(long capacity = 1 * 1024 * 1024)
        {
            this.Capacity = capacity;
            this.Ring = new List<RBuffer>();

            // add two buffers to the ring
            this.Ring.Add(new RBuffer(capacity));
            this.Ring.Add(new RBuffer(capacity));

            this.ActiveBuffer = this.Ring.First();
            this.ActiveBuffer.GoActive();
        }

        void RotateBuffers()
        {
            this.ActiveBuffer.GoFull();
            foreach (var buffer in this.Ring)
            {
                buffer.AttemptToEmpty();
            }
            this.ActiveBuffer = this.Ring.FirstOrDefault(x => x.State == BufferState.Empty);
            if (null == this.ActiveBuffer)
            {
                var newBuffer = new RBuffer(this.Capacity);
                this.Ring.Add(this.ActiveBuffer);
                this.ActiveBuffer = newBuffer;
            }
            this.ActiveBuffer.GoActive();
        }


        public BufferSession Allocate(int size)
        {
            
            var end = Interlocked.Add(ref this.ActiveBuffer.Mark, size);
            var activeBuffer = this.ActiveBuffer;
            if (end > this.ActiveBuffer.Buffer.Length)
            {
                lock (sync)
                {
                    if (end >= this.ActiveBuffer.Buffer.Length)
                    {
                        RotateBuffers();
                    }
                    end = Interlocked.Add(ref this.ActiveBuffer.Mark, size);
                    activeBuffer = this.ActiveBuffer;
                }

            }
            return new BufferSession(this.ActiveBuffer, end - size, size);
        }
                     



    }
}
