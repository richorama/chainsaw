using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Wire;

namespace Chainsaw.Tests
{

    public class Example
    {
        public string Foo { get; set; }
        public int Bar { get; set; }
        public Guid Qux { get; set; }
    }

    [TestClass]
    public class LengthStreamTests
    {
        [TestMethod]
        public void TestLengthIsCorrect()
        {
            var example = new Example { Foo = "FOO", Bar = 42, Qux = Guid.NewGuid() };
            var lengthStream = new LengthStream();

            var serializer = new Serializer();
            serializer.Serialize(example, lengthStream);
            Assert.AreNotEqual(0, lengthStream.Length);

            using (var memoryStream = new MemoryStream())
            {
                serializer.Serialize(example, memoryStream);
                Assert.AreEqual(memoryStream.Length, lengthStream.Length);
            }

        }
    }
}
