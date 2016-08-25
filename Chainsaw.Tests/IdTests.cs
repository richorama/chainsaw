using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chainsaw.Tests
{
    [TestClass]
    public class IdTests
    {
        [TestMethod]
        public void TestGuidGeneration()
        {
            var guid = LogWriter.GenerateGuid(1, 2, 3);
            Assert.AreNotEqual(guid, Guid.Empty);

            var record = guid.ParseRecord();
            Assert.AreEqual(1, record.Generation);
            Assert.AreEqual(2, record.Position);
            Assert.AreEqual(3, record.Length);

        }
    }
}
