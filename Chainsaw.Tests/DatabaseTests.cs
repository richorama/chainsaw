using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;

namespace Chainsaw.Tests
{
    [TestClass]
    public class DatabaseTests
    {
        [TestMethod]
        public void TestDatabase()
        {
            if (Directory.Exists("databasetest")) Directory.Delete("databasetest", true);


            using (var db = new Database<string>("databasetest"))
            {
                for (var i = 0; i < 100; i++)
                {   
                    db.Append(Operation.Append, "key" + i.ToString(), "value" + i.ToString());
                }

                Assert.AreEqual("value57", db.Read("key57"));
            }

            using (var db = new Database<string>("databasetest"))
            {
                Assert.AreEqual("value57", db.Read("key57"));
            }
        }
    }
}
