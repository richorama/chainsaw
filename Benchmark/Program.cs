using Chainsaw;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/*
baseline
    Loading 100000 records took 5120ms
    Opening database took 3141ms
    Snapshotting database took 100ms
    Opening snapshot database took 432ms
    Querying 100000 records took 2787ms

increase log size
    Storing 100000 records took 4400ms
    Opening database took 2629ms
    Snapshotting database took 92ms
    Opening snapshot database took 1624ms
    Querying 100000 records took 2414ms

 */
namespace Benchmark
{

    public struct FakeRecord
    {
        public int Value1 { get; set; }
        public string Value2 { get; set; }
    }

    class Program
    {

        static void Main(string[] args)
        {
            const int recordCount = 100 * 1000;

            if (Directory.Exists("test")) Directory.Delete("test", true);

            var watch = Stopwatch.StartNew();

            using (var db = new Database<FakeRecord>("test"))
            {
                for (var i = 0; i < recordCount; i++)
                {
                    db.Set(i.ToString(), new FakeRecord
                    {
                        Value1 = i,
                        Value2 = Guid.NewGuid().ToString()
                    });
                }
            }

            watch.Stop();
            Console.WriteLine($"Storing {recordCount} records took {watch.ElapsedMilliseconds}ms");
            watch.Reset();
            watch.Start();

            using (var db = new Database<FakeRecord>("test"))
            {
                watch.Stop();
                Console.WriteLine($"Opening database took {watch.ElapsedMilliseconds}ms");
                watch.Reset();
                watch.Start();

                db.SnapshotTheIndex();

                watch.Stop();
                Console.WriteLine($"Snapshotting database took {watch.ElapsedMilliseconds}ms");
                watch.Reset();
            }

            watch.Start();

            using (var db = new Database<FakeRecord>("test"))
            {
                watch.Stop();
                Console.WriteLine($"Opening snapshot database took {watch.ElapsedMilliseconds}ms");
                watch.Reset();

                watch.Start();
                for (var i = 0; i < recordCount; i++)
                {
                    db.Get(i.ToString());
                }

                watch.Stop();
                Console.WriteLine($"Querying {recordCount} records took {watch.ElapsedMilliseconds}ms");

            }

            Console.WriteLine("Press any key to continue . . .");
            Console.ReadKey();
        }

       
    }
}
