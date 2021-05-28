using Microsoft.VisualStudio.TestTools.UnitTesting;
using pageview_processor;
using System.Threading.Tasks;

namespace pageview_processor_tests
{
    [TestClass]
    public class pageview_processor_should
    {
        [TestMethod]
        public async Task work()
        {
            var processor = new DummyProcessor();
            await processor.Process("20200101-000000", "20200101-020000");
        }
    }
}
