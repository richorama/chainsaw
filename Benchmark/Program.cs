using Chainsaw;
using System;
using System.Diagnostics;
using System.IO;

/*
new baseline - new hardware    
    Storing 100000 records took 2055ms
    Opening database took 1246ms
    Snapshotting database took 91ms
    Opening snapshot database took 343ms
    Querying 100000 records took 1176ms

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

            using (var db = new Database<FakeRecord>( "test"))
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
