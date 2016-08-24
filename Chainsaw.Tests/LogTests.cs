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
                Assert.AreEqual("3,100,file1.log", logFile.ToString());
            }
        }

        [TestMethod]
        public void TestLogFileFromString()
        {
            using (var logFile = LogReader.FromString("3,100,file1.log", "."))
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
                    log.Append(new TestPoco { Value = i + 1 });
                }
            }
            using (var log2 = new LogWriter("test", 4 * 1024))
            {
                var counter = 0;
                foreach (var logFile in log2.Files)
                {
                    foreach (var position in logFile.ReadPositions())
                    {
                        var value2 = logFile.Read<TestPoco>(position.Position, position.Length);
                        Assert.IsNotNull(value2);
                        Assert.AreNotEqual(0, value2.Value);
                        counter++;
                    }
                }
                Assert.AreEqual(100, counter);
            }

            using (var log3 = new LogWriter("test", 4 * 1024))
            {
                var result = log3.Read<TestPoco>(1);
                Assert.IsNotNull(result);
                Assert.AreEqual(2, result.Value);
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

            using (var log = new LogWriter("reopen", 4 * 1024))
            {
                Assert.AreEqual(2, log.ActiveFile.ReadPositions().Count());
            }

        }

        [TestMethod]
        public void TestLogDraining()
        {
            if (Directory.Exists("drain")) Directory.Delete("drain", true);
            var buffer = new byte[100];
            for (var i = 0; i < buffer.Length; i++)
            {
                buffer[i] = (byte)i;
            }
            var generationCount = 0;
            Action<LogReader, int> logFull = (_, __) => generationCount++;
            using (var log = new LogWriter("drain", 4 * 1024, logFull))
            {
                for (var i = 0; i < 10000; i++)
                {
                    log.Append(buffer);
                }
                Assert.AreEqual(2, log.Files.Count);
                Assert.AreEqual(generationCount, log.Generation);
            }
            Assert.AreNotEqual(0, generationCount);
            Assert.AreNotEqual(2, generationCount);
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

    }
}
