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


            using (var db = new Database("databasetest"))
            {
                for (var i = 0; i < 100; i++)
                {   
                    db.Append(Operation.Append, Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
                }

             
            }

        }
    }
}
