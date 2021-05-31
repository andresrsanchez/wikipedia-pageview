using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace pageview_processor
{
    public class WikipediaDumpsProcessor
    {
        private const int CAPACITY = 25;
        internal const string FORMAT = "yyyyMMdd-HHmmss";
        private readonly ILogger logger;
        private readonly HttpClient httpClient;
        private static Dictionary<string, HashSet<string>> blackList = new();
        internal static Dictionary<string, string> Cache { get; } = new();

        public WikipediaDumpsProcessor(ILogger logger, HttpClient httpClient = null)
        {
            this.logger = logger;
            this.httpClient = httpClient ?? new HttpClient();
        }

        public async Task<IEnumerable<string>> ProcessAndGetResultsFilePath(string dateFrom, string dateTo)
        {
            var (isValid, parsedDateFrom, parsedDateTo) = DatesValidator.ValidateAndGet(FORMAT, dateFrom, dateTo);
            if (!isValid) throw new Exception();

            if (!blackList.Any()) blackList = await BlackListOfWikipediaDumps.Get(logger, httpClient);

            var datesToProcess = new List<DateTime>();
            var cachedPaths = new List<string>(); //prettify
            for (var date = parsedDateFrom; date <= parsedDateTo; date = date.AddHours(1))
            {
                if (Cache.TryGetValue(date.ToString(FORMAT), out var path)) cachedPaths.Add(path);
                else datesToProcess.Add(date);
            }

            var dumpsLocalPath = await Download(datesToProcess);
            foreach (var (localPath, date) in dumpsLocalPath) Cache.Add(date, localPath);
            cachedPaths.AddRange(await ConsumeAndReturnResults(dumpsLocalPath));

            return cachedPaths;
        }

        internal async Task<IEnumerable<(string localPath, string date)>> Download(IEnumerable<DateTime> datesToProcess)
        {
            var result = new BlockingCollection<(string tempPath, string date)>();
            await Parallel.ForEachAsync(datesToProcess, new ParallelOptions { MaxDegreeOfParallelism = 2 }, async (date, cancellationToken) =>
            {
                var url = @$"https://dumps.wikimedia.org/other/pageviews/{date.Year}/{date:yyyy-MM}/pageviews-{date.ToString(FORMAT)}.gz";
                var response = await httpClient.GetAsync(url, cancellationToken);
                if (!response.IsSuccessStatusCode) throw new Exception("");

                var fileToWriteTo = Path.GetTempFileName();// local system
                {
                    await using var decompressedFileStream = File.Create(fileToWriteTo);
                    await using var responseContent = await response.Content.ReadAsStreamAsync(cancellationToken);
                    await using var decompressionStream = new GZipStream(responseContent, CompressionMode.Decompress);//review usings memory
                    decompressionStream.CopyTo(decompressedFileStream);
                }

                result.Add((fileToWriteTo, date.ToString(FORMAT)), cancellationToken);
            });

            return result.ToList();
        }

        internal async Task<IEnumerable<string>> ConsumeAndReturnResults(IEnumerable<(string localPath, string date)> paths)
        {
            var result = new List<string>();
            foreach (var (localPath, date) in paths)
            {
                await using var fs = File.Open(localPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                await using var bs = new BufferedStream(fs);
                using var sr = new StreamReader(bs);
                string line;

                var top = new Dictionary<string, PriorityQueue<string, int>>();
                while ((line = sr.ReadLine()) != null)
                {
                    var splittedLine = line.Split(' ');
                    if (splittedLine.Length != 4) continue;

                    var domain = splittedLine[0]; var title = splittedLine[1];

                    if (blackList.TryGetValue(domain, out var blackListOfTitles) && blackListOfTitles.Contains(title)) continue;

                    var views = int.Parse(splittedLine[2]);

                    if (top.TryGetValue(domain, out var sortedValues))
                    {
                        if (sortedValues.Count < CAPACITY) sortedValues.Enqueue(title, views);

                        else if (sortedValues.TryPeek(out var _, out var priority) && views > priority)
                        {
                            sortedValues.Dequeue();
                            sortedValues.Enqueue(title, views);
                        }
                    }
                    else
                    {
                        sortedValues = new PriorityQueue<string, int>(CAPACITY);
                        sortedValues.Enqueue(title, views);

                        top.Add(domain, sortedValues);
                    }
                }

                result.Add(await WriteResultsToAFileAndGetPath(date, top));
            }

            return result;
        }

        internal static async Task<string> WriteResultsToAFileAndGetPath(string date, Dictionary<string, PriorityQueue<string, int>> results)
        {
            if (!Directory.Exists("/dumps")) Directory.CreateDirectory("/dumps");

            var path = @$"/dumps/{date}";
            var stringbuilder = new StringBuilder();
            foreach (var result in results.ToList())
            {
                var queue = result.Value;
                var domain = result.Key;

                var stack = new Stack<string>();
                while (queue.TryDequeue(out string item, out int priority)) stack.Push($"{domain} {item} {priority}");
                foreach (var item in stack) stringbuilder.AppendLine(item);
            }

            if (File.Exists(path)) File.Delete(path);

            await File.AppendAllTextAsync(path, stringbuilder.ToString());
            return path;
        }
    }
}
