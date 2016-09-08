using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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
                    db.Append("key" + i.ToString(), "value" + i.ToString());
                }

                Assert.AreEqual("value57", db.Read("key57"));
            }

            using (var db = new Database<string>("databasetest"))
            {
                Assert.AreEqual("value57", db.Read("key57"));
            }

            using (var db = new Database<string>("databasetest"))
            {
                Assert.AreEqual(100, db.Scan().Count());
            }

            using (var db = new Database<string>("databasetest"))
            {
                db.Delete("key12");
                Assert.AreEqual(101, db.Scan().Count());
                var value = db.Read("key12");
                Assert.AreEqual(null, value);
            }


        }

        [TestMethod]
        public void TestIndexLoad()
        {
            if (Directory.Exists("TestIndexLoad")) Directory.Delete("TestIndexLoad", true);

            using (var db = new Database<int>("TestIndexLoad"))
            {
                db.Append("one", 99);
                db.Append("two", 2);

                db.SnapshotTheIndex();

                db.Append("three", 3);
                db.Append("four", 4);
                db.Append("one", 1);
            }
            using (var db = new Database<int>("TestIndexLoad"))
            {
                Assert.AreEqual(4, db.Count);
                Assert.AreEqual(1, db.Read("one"));
                Assert.AreEqual(2, db.Read("two"));
                Assert.AreEqual(3, db.Read("three"));
                Assert.AreEqual(4, db.Read("four"));
            }
        }
            

    }
}
