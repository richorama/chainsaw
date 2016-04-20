using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chainsaw.Tests
{
    [TestClass]
    public class BufferTests
    {
        [TestMethod]
        public void TestBasicBuffer()
        {
            var buffer = new BufferRing();
            for (var i = 0; i < 10000; i++)
            {
                using (var allocation = buffer.Allocate(1000))
                {
                    Assert.IsNotNull(allocation);
                    Assert.AreEqual(1000, allocation.Length);
                    Assert.IsNotNull(allocation.Buffer.Buffer);
                }
            }
            Assert.AreEqual(2, buffer.Ring.Count);
            Assert.AreEqual(1, buffer.Ring.Count(x => x.State == BufferState.Active));
        }
    }
}
