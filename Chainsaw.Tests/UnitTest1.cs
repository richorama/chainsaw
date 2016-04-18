using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Threading;
using System.Collections.Generic;
using System.Diagnostics;

namespace Chainsaw.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestLogFileToString()
        {
            using (var logFile = new LogFile(".", "file1.log", 100, LogState.Full))
            {
                Assert.AreEqual("3,100,file1.log", logFile.ToString());
            }
        }

        [TestMethod]
        public void TestLogFileFromString()
        {
            using (var logFile = LogFile.FromString("3,100,file1.log", "."))
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

            var rand = new Random();
            long index = -1;

            using (var log = new Log("test", 4 * 1024))
            {
                for (var i = 0; i < 100; i++)
                {
                    var buffer = new byte[rand.Next(100) + 1];
                    for (var j = 0; j < buffer.Length; j++)
                    {
                        buffer[j] = (byte)j;
                    }

                    // ensure that the high water mark is being incremented
                    var position = log.Append(buffer);

                    Assert.AreEqual(buffer.Length, position.Length);

                    index = position.Position;
                }
            }
            using (var log2 = new Log("test", 4 * 1024))
            {
                var buffer2 = new byte[101];
                index = -1;
                var counter = 0;
                foreach (var logFile in log2.Files)
                {
                    foreach (var position in logFile.ReadPositions())
                    {
                        logFile.ReadBuffer(position, buffer2);
                        index = position.Position;
                        Assert.AreNotEqual(0, position.Length);
                        for (var i = 0; i < position.Length; i++)
                        {
                            Assert.AreEqual(i, buffer2[i]);
                        }
                        counter++;
                    }
                }
                Assert.AreEqual(100, counter);
            }

        }

        [TestMethod]
        public void TestReopeningActiveFile()
        {
            if (Directory.Exists("reopen")) Directory.Delete("reopen", true);

            using (var log = new Log("reopen", 4 * 1024))
            {
                var buffer = new byte[] { 1 };
                log.Append(buffer);
            }

            using (var log = new Log("reopen", 4 * 1024))
            {
                var buffer = new byte[] { 2 };
                log.Append(buffer);
            }

            using (var log = new Log("reopen", 4 * 1024))
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
            Action<LogFile> logFull = x => generationCount++;
            using (var log = new Log("drain", 4 * 1024, logFull))
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

            using (var log = new Log("sat", 4 * 1024 * 1024))
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


            using (var log = new Log("raw", 4 * 1024 * 1024))
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
