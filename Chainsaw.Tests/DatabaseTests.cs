using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
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

            using (var db = new Database("databasetest"))
            {
                for (var i = 0; i < 100; i++)
                {
                    var key = new byte[i];
                    var value = new byte[i];

                    for (var j = 0; j < i; j++)
                    {
                        key[j] = (byte)j;
                        value[j] = (byte)i;
                    }
                    
                    db.Append(Operation.Append, key, 0, key.Length, value, 0, value.Length);
                }
            }
        }
    }
}
