using FluentAssertions;
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

            var path = await WikipediaDumpsProcessor.WriteResultsToAFileAndGetPath("20200201-000000", results);
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

            var wikipediaDumpsProcessor = new WikipediaDumpsProcessor(default, default);
            var downloadPaths = await wikipediaDumpsProcessor.Download(dates);

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

            var wikipediaDumpsProcessor = new WikipediaDumpsProcessor(default, default);
            var downloadPaths = await wikipediaDumpsProcessor.Download(dates);

            var resultsPath = (await wikipediaDumpsProcessor.ConsumeAndReturnResults(downloadPaths)).ToArray();

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
