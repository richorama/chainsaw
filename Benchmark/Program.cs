﻿using Chainsaw;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/*
[Ctrl] + [F5]
300000 in 5128ms
300000 in 6365ms // with wire serialization, no buffer ring
300000 in 5789ms // switch to a struct for record position
300000 in 9335ms // with guids
300000 in 8852ms // optimise guid generation
300000 in 6168ms // remove dodgy extra new serializer()
300000 in 6261ms // upgraded wire to 0.8
300000 in 5797ms // disable the clean
*/


namespace Benchmark
{
    class Program
    {

        static void Main(string[] args)
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
