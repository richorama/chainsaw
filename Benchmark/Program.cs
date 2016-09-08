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


/*

inserting data
    92633 ms (1734489 records)
reopening
    52528 ms

with an index snapshot
    9383 ms 
    5132 ms
 */
namespace Benchmark
{

    public struct PostcodeRecord
    {
        public int Id { get; set; }
        public string Postcode { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }

        public override string ToString()
        {
            return $"{this.Postcode}, {this.Latitude},{this.Longitude}";
        }
    }

    class Program
    {

        static IEnumerable<string> ReadLines(string filename)
        {
            string line = null;

            using (var file = new StreamReader(filename))
            {
                while ((line = file.ReadLine()) != null)
                {
                    yield return line;
                }
            }
        }


        static void Main(string[] args)
        {
            //if (Directory.Exists("ratest")) Directory.Delete("ratest", true);
            var watch = Stopwatch.StartNew();
            var counter = 0;
            using (var db = new Database<PostcodeRecord>("ratest", 40 * 1024 * 1024))
            {
                
                /*
                Console.WriteLine("inserting data");
                foreach (var line in ReadLines("ukpostcodes.csv").Skip(1))
                {
                    var parts = line.Split(',');
                    var record = new PostcodeRecord
                    {
                        Id = int.Parse(parts[0]),
                        Postcode = parts[1],
                        Latitude = double.Parse(parts[2]),
                        Longitude = double.Parse(parts[3])
                    };

                    db.Append(record.Postcode.Replace(" ", ""), record);
                    counter++;
                }
                */
                watch.Stop();
                Console.WriteLine($"load time: {watch.ElapsedMilliseconds} ms ({counter} records)");
                watch.Reset();

                Console.WriteLine("snapshot");
                //db.SnapshotTheIndex();
                Console.WriteLine("done");

            }



            Console.WriteLine("reopening");
            watch.Start();
            using (var db = new Database<PostcodeRecord>("ratest", 40 * 1024 * 1024))
            {
                watch.Stop();
                Console.WriteLine($"{watch.ElapsedMilliseconds} ms");
                Console.WriteLine("querying");
                Console.WriteLine(db.Read("IP124DH"));

            }
            Console.WriteLine("Press any key to continue . . .");
            Console.ReadKey();
        }

        /*
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
        }*/
    }
}
