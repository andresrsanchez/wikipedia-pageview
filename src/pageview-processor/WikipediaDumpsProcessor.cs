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
using System.Threading.Channels;
using System.Threading.Tasks;

namespace pageview_processor
{
    public class WikipediaDumpsProcessor
    {
        private const int CAPACITY = 25;
        internal const string OLD_FORMAT = "yyyyMMdd-HH0000"; //This format is to avoid issues with names on local filesystem
        internal const string FORMAT = "yyyy-MM-ddTHH:00:00";
        private const string PATH = "/dumps";
        private readonly ILogger logger;
        private readonly IDumpsCache cache;
        private readonly HttpClient httpClient;
        private string resultsPath = PATH;
        private static Dictionary<string, HashSet<string>> blackList = new();
        internal readonly Channel<(string localPath, string date)> channel = Channel.CreateUnbounded<(string localPath, string date)>();

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

            var stopWatch = new Stopwatch(); stopWatch.Start();
            logger.LogInformation($"Start processing from: {parsedDateFrom.ToString(FORMAT)} to: {parsedDateTo.ToString(FORMAT)} and path: {resultsPath}");
            if (!blackList.Any()) blackList = await BlackListOfWikipediaDumps.Get(logger, httpClient); //Should we persist blacklist?

            this.resultsPath = resultsPath;
            var datesToProcess = new List<DateTime>();
            var datesProcessed = new List<string>();
            for (var date = parsedDateFrom; date <= parsedDateTo; date = date.AddHours(1))
            {
                if (cache.TryGet(date.ToString(OLD_FORMAT), out var path))
                {
                    logger.LogInformation($"Getting date: {date.ToString(FORMAT)} with path: {path} from cache");
                    datesProcessed.Add(path);
                }
                else datesToProcess.Add(date);
            }

            var consumer = ConsumeAndReturnResults();
            var producer = Download(datesToProcess);
            await foreach (var path in consumer) { logger.LogInformation($"datesProcessed: {path}"); datesProcessed.Add(path); }

            await producer;
            
            logger.LogInformation(@$"End processing from: {parsedDateFrom.ToString(FORMAT)} to: {parsedDateTo.ToString(FORMAT)} in: {Math.Round(stopWatch.Elapsed.TotalSeconds, 2)} seconds");

            return datesProcessed;
        }

        // MaxDegreeOfParallelism of 5 is because wikipedia dumps server returns a lot of 503 status code
        // with a high number of concurrent http connections
        internal async Task Download(List<DateTime> datesToProcess)
        {
            var exceptions = new ConcurrentQueue<Exception>();
            await Parallel.ForEachAsync(datesToProcess, new ParallelOptions { MaxDegreeOfParallelism = 5 }, async (date, cancellationToken) =>
            {
                try
                {
                    var stopWatch = new Stopwatch(); stopWatch.Start();
                    logger.LogInformation($"Starting the download of date: {date.ToString(FORMAT)}");

                    var url = @$"https://dumps.wikimedia.org/other/pageviews/{date.Year}/{date:yyyy-MM}/pageviews-{date.ToString(OLD_FORMAT)}.gz";
                    var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                    if (!response.IsSuccessStatusCode)
                    {
                        logger.LogError(@$"Error downloading date: {date.ToString(FORMAT)} with status code: {response.StatusCode}");
                        return;
                    }
                    var fileToWriteTo = Path.GetTempFileName();
                    {
                        await using var decompressedFileStream = File.Create(fileToWriteTo);
                        await using var responseContent = await response.Content.ReadAsStreamAsync(cancellationToken);
                        await using var decompressionStream = new GZipStream(responseContent, CompressionMode.Decompress);
                        decompressionStream.CopyTo(decompressedFileStream);
                    }
                    await channel.Writer.WriteAsync((fileToWriteTo, date.ToString(OLD_FORMAT)), cancellationToken);

                    logger.LogInformation($"Ending the download of date: {date.ToString(FORMAT)} in: {Math.Round(stopWatch.Elapsed.TotalSeconds, 2)} seconds");
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });
            channel.Writer.Complete();

            if (!exceptions.IsEmpty) throw new AggregateException(exceptions);
        }

        internal async IAsyncEnumerable<string> ConsumeAndReturnResults()
        {
            while (await channel.Reader.WaitToReadAsync())
            {
                if (channel.Reader.TryRead(out (string localPath, string date) paths))
                {
                    var localPath = paths.localPath; var date = paths.date;
                    logger.LogInformation($"Start consuming file with localPath: {localPath} and date: {date}");

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
                            //Specify capacity to reduce the number of re-allocations
                            sortedValues = new PriorityQueue<string, int>(CAPACITY);
                            sortedValues.Enqueue(title, views);

                            top.Add(domain, sortedValues);
                        }
                    }

                    logger.LogInformation($"Ending file consumption with date: {date}");
                    yield return await WriteResultsToAFileAndGetPath(date, top);
                }
            }
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

                //TODO: We need to address the invert of the whole priority queue problem 
                //This is done to get the results in descending order
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
