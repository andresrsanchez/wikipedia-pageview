using pageview_processor;
using System;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var wikipediaDumpsProcessor = new WikipediaDumpsProcessor(default, default);
            await wikipediaDumpsProcessor.ProcessAndGetResultsFilePath("20200101-000000", "20200101-090000");
        }
    }
}
