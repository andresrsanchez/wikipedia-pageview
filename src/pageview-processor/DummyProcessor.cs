using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace pageview_processor
{
    public class DummyProcessor
    {
        const string format = "yyyyMMdd-HHmmss";
        private readonly HttpClient _httpClient;
        private static Dictionary<string, HashSet<string>> blackList = default;

        public DummyProcessor(HttpClient httpClient = null)
        {
            _httpClient = httpClient ?? new HttpClient();
        }

        public class DuplicateKeyComparer<TKey> : IComparer<TKey> where TKey : IComparable
        {
            public int Compare(TKey x, TKey y)
            {
                var result = y.CompareTo(x);     // Handle equality as being greater. Note: this will break Remove(key) or
                return result == 0 ? 1 : result; // IndexOfKey(key) since the comparer never returns 0 to signal key equality
            }
        }

        public async Task Process(string from, string to)
        {
            var (isValid, dateFrom, dateTo) = ValidateDates(from, to);
            if (!isValid) throw new Exception();

            blackList = await Init(); //null initialization

            var tempFileNames = new List<string>();
            for (DateTime date = dateFrom; date < dateTo; date = date.AddHours(1))
            {
                var url = @$"https://dumps.wikimedia.org/other/pageviews/{date.Year}/{date:yyyy-MM}/pageviews-{date.ToString(format)}.gz";
                var response = await _httpClient.GetAsync(url);//pagecount latest format

                if (!response.IsSuccessStatusCode) throw new Exception(""); //WTF do¿?

                var fileToWriteTo = Path.GetTempFileName();// local system
                await using FileStream decompressedFileStream = File.Create(fileToWriteTo);
                await using var decompressionStream = new GZipStream(await response.Content.ReadAsStreamAsync(), CompressionMode.Decompress);
                decompressionStream.CopyTo(decompressedFileStream);

                tempFileNames.Add(fileToWriteTo);
            }

            //fucking waste of time

            var top = new Dictionary<string, SortedList<int, string>>();

            foreach (var tempFileName in tempFileNames)
            {
                //var hugeFile = await File.ReadAllLinesAsync(tempFileName); //memory is free!!!
                //foreach (var line in hugeFile)
                //{


                await using var fs = File.Open(tempFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                await using var bs = new BufferedStream(fs);
                using var sr = new StreamReader(bs);
                string line;
                while ((line = sr.ReadLine()) != null) //is readline performant?
                {
                    var splittedLine = line.Split(' ');
                    if (splittedLine.Length != 4) throw new Exception();

                    if (blackList.TryGetValue(splittedLine[0], out var titles) && titles.Contains(splittedLine[1])) continue;

                    if(top.TryGetValue(splittedLine[0], out var sortedValues))
                    {
                        sortedValues.Add(int.Parse(splittedLine[2]), splittedLine[1]);
                        top[splittedLine[0]] = sortedValues; //is this neccesary¿??
                    }
                    else
                    {
                        sortedValues = new SortedList<int, string>(25, new DuplicateKeyComparer<int>())
                        {
                            { int.Parse(splittedLine[2]), splittedLine[1] }
                        };

                        top.Add(splittedLine[0], sortedValues);
                    }

                }
            }
        }

        async Task<Dictionary<string, HashSet<string>>> Init()
        {
            var result = new Dictionary<string, HashSet<string>>();
            var response = await _httpClient.GetAsync(@"https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages"); //is it a gz file¿??
            ////throw httpclient exception¿?

            var content = await response.Content.ReadAsStringAsync(); //loading all into memory
            foreach (var line in content.Split(new[] { '\r', '\n' }).Where(x => !string.IsNullOrWhiteSpace(x)))
            {
                var splittedLine = line.Split(' ');
                
                if (splittedLine.Length != 2)
                    throw new Exception();

                if (result.TryGetValue(splittedLine[0], out var titles))
                {
                    titles.Add(splittedLine[1]);
                }
                else
                {
                    titles = new HashSet<string>
                    {
                        splittedLine[1]
                    };

                    result.Add(splittedLine[0], titles);
                }
                    
            }

            return result;
        }

        static (bool isValid, DateTime from, DateTime to) ValidateDates(string from, string to)
        {
            static (bool isValid, DateTime date) Parse(string date)
            {//validate null
                var isValid = DateTime.TryParseExact(date, format,
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out var parseDate);

                return (isValid, parseDate);
            }

            var fromDate = Parse(from);
            var toDate = Parse(to);

            return (fromDate.isValid && toDate.isValid, fromDate.date, toDate.date);
        }
    }
}
