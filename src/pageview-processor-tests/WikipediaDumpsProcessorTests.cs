using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using pageview_processor;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace pageview_processor_tests
{
    [TestClass]
    public class wikipedia_dumps_processor_should
    {
        private readonly WikipediaDumpsProcessor processor;
        private readonly IDumpsCache cache;

        public wikipedia_dumps_processor_should()
        {
            var service = new ServiceCollection()
                .AddHttpClient()
                .AddLogging()
                .AddTransient<IDumpsCache, SQLiteDumpsCache>()
                .AddTransient<WikipediaDumpsProcessor>()
                .BuildServiceProvider();
            processor = service.GetRequiredService<WikipediaDumpsProcessor>();
            cache = service.GetRequiredService<IDumpsCache>();
        }

        [TestMethod]
        public async Task cache_correctly()//do we need cache flush?
        {
            await processor.ProcessAndGetResultsFilePath("20200203-010000", "20200203-020000");

            cache.TryGet("20200203-010000", out var _).Should().BeTrue();

            await processor.ProcessAndGetResultsFilePath("20200203-010000", "20200203-020000");
        }

        [TestMethod]
        public async Task write_ordered_results_to_a_file()
        {
            var firstSet = new PriorityQueue<string, int>();
            firstSet.Enqueue("User:Klangtao", 2);
            firstSet.Enqueue("Template:Annotate", 3);

            var secondSet = new PriorityQueue<string, int>();
            secondSet.Enqueue("User:Klangtao", 2);
            secondSet.Enqueue("Template:Annotate", 3);

            var results = new Dictionary<string, PriorityQueue<string, int>>
            {
                {"aa.d", firstSet },
                {"ab", secondSet },
            };

            var path = await processor.WriteResultsToAFileAndGetPath("20200201-000000", results);
            var lines = await File.ReadAllLinesAsync(path);

            lines[0].Should().Be("aa.d Template:Annotate 3");
            lines[1].Should().Be("aa.d User:Klangtao 2");
            lines[2].Should().Be("ab Template:Annotate 3");
            lines[3].Should().Be("ab User:Klangtao 2");
        }

        [TestMethod]
        public async Task download_correctly()
        {
            var dates = new List<DateTime>
            {
                new DateTime(2020, 01, 01, 01, 00, 00),
                new DateTime(2020, 01, 01, 02, 00, 00)
            };

            var downloadPaths = await processor.Download(dates);

            downloadPaths.Should().HaveCount(2);

            foreach (var downloadPath in downloadPaths)
                File.Exists(downloadPath.localPath).Should().BeTrue();
        }

        [TestMethod]
        public async Task consume_and_return_results_correctly()
        {
            var dates = new List<DateTime>
            {
                new DateTime(2020, 01, 01, 03, 00, 00),
                new DateTime(2020, 01, 01, 04, 00, 00)
            };

            var downloadPaths = await processor.Download(dates);

            var resultsPath = (await processor.ConsumeAndReturnResults(downloadPaths)).ToArray();

            static void CheckFirstTwoLinesByFilePath(string path, string firstLine, string secondLine)
            {
                using var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var bs = new BufferedStream(fs);
                using var sr = new StreamReader(bs);

                firstLine.Should().Be(sr.ReadLine());
                secondLine.Should().Be(secondLine);
            }

            downloadPaths.Should().HaveSameCount(resultsPath);

            var firstFile = resultsPath
                .First(x => Path.GetFileName(x) == dates[0].ToString(WikipediaDumpsProcessor.FORMAT));
            var secondFile = resultsPath
               .First(x => Path.GetFileName(x) == dates[1].ToString(WikipediaDumpsProcessor.FORMAT));

            CheckFirstTwoLinesByFilePath(firstFile, "aa Wikipedia:Community_Portal 3", "aa Main_Page 2");
            CheckFirstTwoLinesByFilePath(secondFile, "aa Main_Page 17", "aa Special:CiteThisPage 2");
        }
    }
}
