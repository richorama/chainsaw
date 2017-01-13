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
                    db.Set("key" + i.ToString(), "value" + i.ToString());
                }

                Assert.AreEqual("value57", db.Get("key57"));
            }

            using (var db = new Database<string>("databasetest"))
            {
                Assert.AreEqual("value57", db.Get("key57"));
            }

            using (var db = new Database<string>("databasetest"))
            {
                Assert.AreEqual(100, db.Scan().Count());
            }

            using (var db = new Database<string>("databasetest"))
            {
                db.Delete("key12");
                Assert.AreEqual(101, db.Scan().Count());
                var value = db.Get("key12");
                Assert.AreEqual(null, value);
            }


        }

        [TestMethod]
        public void TestIndexLoad()
        {
            if (Directory.Exists("TestIndexLoad")) Directory.Delete("TestIndexLoad", true);

            using (var db = new Database<int>("TestIndexLoad"))
            {
                db.Set("one", 99);
                db.Set("two", 2);

                db.SnapshotTheIndex();

                db.Set("three", 3);
                db.Set("four", 4);
                db.Set("one", 1);
            }
            using (var db = new Database<int>("TestIndexLoad"))
            {
                Assert.AreEqual(4, db.Count);
                Assert.AreEqual(1, db.Get("one"));
                Assert.AreEqual(2, db.Get("two"));
                Assert.AreEqual(3, db.Get("three"));
                Assert.AreEqual(4, db.Get("four"));
            }
        }

        [TestMethod]
        public void TestBatchOperation()
        {
            if (Directory.Exists("TestBatchOperation")) Directory.Delete("TestBatchOperation", true);

            using (var db = new Database<int>("TestBatchOperation"))
            {
                db.Set("one", 99);
                db.Set("zero", -1);

                var batch = db.CreateBatch();
                batch.Set("one", 1);
                batch.Set("two", 2);
                batch.Set("three", 3);
                batch.Delete("zero");

                Assert.AreEqual(99, db.Get("one"));
                Assert.AreEqual(0, db.Get("three"));
                Assert.AreEqual(-1, db.Get("zero"));

                batch.Commit();

                Assert.AreEqual(1, db.Get("one"));
                Assert.AreEqual(3, db.Get("three"));
                Assert.AreEqual(0, db.Get("zero"));

            }
        }

        [TestMethod]
        public void SecondaryIndex()
        {
            if (Directory.Exists("SecondaryIndex")) Directory.Delete("SecondaryIndex", true);

            using (var db = new Database<string>("SecondaryIndex"))
            {
                db.Set("a", "A");
                db.Set("b", "B");

                var query = db.RegisterSecondaryIndex<int>(x => x.Length);

                db.Set("c", "CC");
                db.Set("d", "DDD");

                var results = query(1).ToArray();
                Assert.AreEqual(2, results.Length);
                Assert.IsTrue(results.Contains("A"));
                Assert.IsTrue(results.Contains("B"));

                results = query(2).ToArray();
                Assert.AreEqual(1, results.Length);
                Assert.IsTrue(results.Contains("CC"));

                results = query(3).ToArray();
                Assert.AreEqual(1, results.Length);
                Assert.IsTrue(results.Contains("DDD"));
            }

        }
    }
}
