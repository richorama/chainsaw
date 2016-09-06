using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Threading;
using System.Collections.Generic;
using System.Diagnostics;

namespace Chainsaw.Tests
{

    public class TestPoco
    {
        public int Value { get; set; }
    }

    [TestClass]
    public class LogTests
    {
        [TestMethod]
        public void TestLogFileToString()
        {
            using (var logFile = new LogReader(".", "file1.log", 100, LogState.Full))
            {
                Assert.AreEqual("2,100,file1.log", logFile.ToString());
            }
        }

        [TestMethod]
        public void TestLogFileFromString()
        {
            using (var logFile = LogReader.FromString("2,100,file1.log", "."))
            {
                Assert.AreEqual(100, logFile.Capacity);
                Assert.AreEqual(LogState.Full, logFile.State);
                Assert.AreEqual("file1.log", logFile.Filename);
            }
        }

        [TestMethod]
        public void TestBasicCrud()
        {
          
            if (Directory.Exists("test")) Directory.Delete("test", true);

            using (var log = new LogWriter("test", 4 * 1024))
            {
                for (var i = 0; i < 100; i++)
                {
                    var poco = new TestPoco { Value = i + 1 };
                    var guid = log.Append(poco);
                    var poco2 = log.Read<TestPoco>(guid);

                    Assert.AreEqual(poco.Value, poco2.Value);
                }
            }

            using (var log2 = new LogWriter("test", 4 * 1024))
            {
                var counter = 0;
                foreach (var logFile in log2.Files)
                {
                    foreach (var position in logFile.ReadPositions(0))
                    {
	                    var record = position.ParseRecord();
                        var value2 = logFile.Read<TestPoco>(record.Position, record.Length);
                        Assert.IsNotNull(value2);
                        Assert.AreNotEqual(0, value2.Value);
                        counter++;
                    }
                }
                Assert.AreEqual(100, counter);
            }

            using (var log = new LogWriter("test", 4 * 1024))
			{
				Assert.AreEqual(100, log.ReadAllKeys().Count());
			}

            using (var log = new LogWriter("test", 4 * 1024))
            {

                var key = log.ReadAllKeys().Skip(50).First();
                Assert.AreEqual(50, log.ReadAllKeys(key).Count());
            }

        }

        [TestMethod]
        public void TestReopeningActiveFile()
        {
            if (Directory.Exists("reopen")) Directory.Delete("reopen", true);

            using (var log = new LogWriter("reopen", 4 * 1024))
            {
                var buffer = new byte[] { 1 };
                log.Append(buffer);
            }

            using (var log = new LogWriter("reopen", 4 * 1024))
            {
                var buffer = new byte[] { 2 };
                log.Append(buffer);
            }



        }

        [TestMethod]
        public void TestSaturation()
        {
            if (Directory.Exists("sat")) Directory.Delete("sat", true);

            var rand = new Random();

            var parallelism = 4;
            var batch = 20000;

            using (var log = new LogWriter("sat", 4 * 1024 * 1024))
            {
                var threads = new List<Thread>();

                for (var k = 0; k < parallelism; k++)
                {
                    threads.Add(new Thread(() => {
                        for (var i = 0; i < batch; i++)
                        {
                            var buffer = new byte[rand.Next(100) + 1];
                            for (var j = 0; j < buffer.Length; j++)
                            {
                                buffer[j] = (byte)j;
                            }
                            try
                            {
                                log.Append(buffer);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.ToString());
                            }
                        }
                    }));
                }

                foreach (var thread in threads)
                {
                    thread.Start();
                }

                foreach (var thread in threads)
                {
                    thread.Join();
                }


            }
        }



        [TestMethod]
        public void TestRawThroughput()
        {
            if (Directory.Exists("raw")) Directory.Delete("raw", true);

            var rand = new Random();
            var parallelism = Environment.ProcessorCount;
            var batch = 300000 / parallelism;


            using (var log = new LogWriter("raw", 4 * 1024 * 1024))
            {
                var threads = new List<Thread>();
                var buffer = new byte[] { 1, 2 };

                for (var k = 0; k < parallelism; k++)
                {
                    threads.Add(new Thread(() => {
                        for (var i = 0; i < batch; i++)
                        {
                            try
                            {
                                log.Append(buffer);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.ToString());
                            }
                        }
                    }));
                }

                var watch = Stopwatch.StartNew();

                foreach (var thread in threads)
                {
                    thread.Start();
                }

                foreach (var thread in threads)
                {
                    thread.Join();
                }

                watch.Stop();

                Console.WriteLine($"{parallelism * batch} in {watch.ElapsedMilliseconds}ms");
            }
        }



        [TestMethod]
        public void TestBatch()
        {

            if (Directory.Exists("test")) Directory.Delete("test", true);

            using (var log = new LogWriter("test", 4 * 1024))
            {
                var pocos = new List<TestPoco>();
                for (var i = 0; i < 10; i++)
                {
                    pocos.Add(new TestPoco { Value = i + 1 });

                }

                var guids = log.Batch(pocos.ToArray());

                Assert.AreEqual(10, guids.Length);
                foreach (var guid in guids)
                {
                    var value = log.Read<TestPoco>(guid);
                    Assert.AreNotEqual(0, value.Value);
                }

            }


        }


    }
}
