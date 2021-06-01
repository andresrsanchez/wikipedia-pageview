using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        internal const string FORMAT = "yyyyMMdd-HH0000";
        private const string PATH = "/dumps";
        private readonly ILogger logger;
        private readonly IDumpsCache cache;
        private readonly HttpClient httpClient;
        private string resultsPath = PATH;
        private static Dictionary<string, HashSet<string>> blackList = new();

        public WikipediaDumpsProcessor(ILogger<WikipediaDumpsProcessor> logger, IDumpsCache cache, IHttpClientFactory httpclientFactory = null)
        {
            this.logger = logger;
            this.cache = cache;
            this.httpClient = httpclientFactory?.CreateClient() ?? new HttpClient();
        }

        public async Task<IEnumerable<string>> ProcessAndGetResultsFilePath(string dateFrom, string dateTo, string resultsPath = PATH)
        {
            var (isValid, parsedDateFrom, parsedDateTo) = DatesValidator.ValidateAndGet(FORMAT, dateFrom, dateTo);
            if (!isValid) throw new Exception($"Invalid dates, cannot process from: {dateFrom} to: {dateTo}");

            logger.LogInformation($"Start processing from: {parsedDateFrom.ToString(FORMAT)} to: {parsedDateTo.ToString(FORMAT)} and path: {resultsPath}");
            if (!blackList.Any()) blackList = await BlackListOfWikipediaDumps.Get(logger, httpClient);

            this.resultsPath = resultsPath;
            var datesToProcess = new List<DateTime>();
            var datesProcessed = new List<string>(); //prettify
            for (var date = parsedDateFrom; date <= parsedDateTo; date = date.AddHours(1))
            {
                if (cache.TryGet(date.ToString(FORMAT), out var path))
                {
                    logger.LogInformation($"Getting date: {date.ToString(FORMAT)} with path: {path} from cache");
                    datesProcessed.Add(path);
                }
                else datesToProcess.Add(date);
            }

            var dumpsLocalPath = await Download(datesToProcess);
            foreach (var (localPath, date) in dumpsLocalPath) cache.Add(date, localPath);
            datesProcessed.AddRange(await ConsumeAndReturnResults(dumpsLocalPath));

            return datesProcessed;
        }

        internal async Task<IEnumerable<(string localPath, string date)>> Download(List<DateTime> datesToProcess)
        {
            var result = new BlockingCollection<(string tempPath, string date)>();
            await Parallel.ForEachAsync(datesToProcess, new ParallelOptions { MaxDegreeOfParallelism = 3 }, async (date, cancellationToken) =>
            {
                var stopWatch = new Stopwatch(); stopWatch.Start();
                logger.LogInformation($"Starting the download of date: {date.ToString(FORMAT)}");

                var url = @$"https://dumps.wikimedia.org/other/pageviews/{date.Year}/{date:yyyy-MM}/pageviews-{date.ToString(FORMAT)}.gz";
                var response = await httpClient.GetAsync(url, cancellationToken);
                if (!response.IsSuccessStatusCode) 
                    throw new Exception(@$"Error downloading date: {date.ToString(FORMAT)} with status code: {response.StatusCode}");

                var fileToWriteTo = Path.GetTempFileName();
                {
                    await using var decompressedFileStream = File.Create(fileToWriteTo);
                    await using var responseContent = await response.Content.ReadAsStreamAsync(cancellationToken);
                    await using var decompressionStream = new GZipStream(responseContent, CompressionMode.Decompress);//review usings memory
                    decompressionStream.CopyTo(decompressedFileStream);
                }
                result.Add((fileToWriteTo, date.ToString(FORMAT)), cancellationToken);

                logger.LogInformation($"Ending the download of date: {date.ToString(FORMAT)} in: {Math.Round(stopWatch.Elapsed.TotalSeconds, 2)} seconds");
            });

            return result.ToList();
        }

        internal async Task<IEnumerable<string>> ConsumeAndReturnResults(IEnumerable<(string localPath, string date)> paths)
        {
            var result = new List<string>();
            foreach (var (localPath, date) in paths)
            {
                logger.LogInformation($"Start consuming file with localPath: {localPath} and date: {date}");//include times?

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

                logger.LogInformation($"Ending file consumption");
                result.Add(await WriteResultsToAFileAndGetPath(date, top));
            }

            return result;
        }

        internal async Task<string> WriteResultsToAFileAndGetPath(string date, Dictionary<string, PriorityQueue<string, int>> results)
        {
            logger.LogInformation($"Start writing result to a file with date: {date}");

            if (!Directory.Exists(resultsPath)) Directory.CreateDirectory(resultsPath);

            var path = @$"{resultsPath}/{date}";
            var stringbuilder = new StringBuilder();
            foreach (var result in results.ToList())
            {
                var queue = result.Value;
                var domain = result.Key;

                var stack = new Stack<string>();
                while (queue.TryDequeue(out string item, out int priority)) stack.Push($"{domain} {item} {priority}");
                foreach (var item in stack) stringbuilder.AppendLine(item);
            }

            if (File.Exists(path))
            {
                logger.LogWarning($"The file with path: {path} already exists!");
                File.Delete(path);
            }

            await File.AppendAllTextAsync(path, stringbuilder.ToString());
            logger.LogInformation($"Ending writing result to a file with date: {date} and path: {path}");

            return path;
        }
    }
}
