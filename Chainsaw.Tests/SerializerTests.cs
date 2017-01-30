using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Chainsaw.Tests
{

    public class TestPoco2
    {
        public int Value { get; set; }
    }

    [TestClass]
    public class SerializerTests
    {
        [TestMethod]
        public void TestSerializer()
        {
            using (var stream = new MemoryStream())
            {
                TestSerializer(stream);

            }
        }

        private static void TestSerializer(Stream stream)
        {
            var serializer = new HyperionSerializer();
            serializer.Serialize(new TestPoco2 { Value = 42 }, stream);
            stream.Position = 0;
            stream.Flush();
            var output = serializer.Deserialize<TestPoco2>(stream);
            Assert.IsNotNull(output);
            Assert.AreEqual(42, output.Value);
        }

        [TestMethod]
        public void TestViewAccessorStreamWithSerializer()
        {
            var tempFilename = Path.GetTempFileName();
            var file = MemoryMappedFile.CreateFromFile(tempFilename, FileMode.OpenOrCreate, "test.bin", 49);
            using (var stream = file.CreateViewStream(0,49))
            {
                TestSerializer(stream);
            }

        }
    }
}
