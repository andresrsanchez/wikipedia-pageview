using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace pageview_processor
{
    internal static class BlackListOfWikipediaDumps
    {
        internal static async Task<Dictionary<string, HashSet<string>>> Get(ILogger logger, HttpClient httpClient)
        {
            logger.LogInformation("Initializing blacklist");
            var response = await httpClient.GetAsync(@"https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages");
            if (!response.IsSuccessStatusCode) throw new Exception($"Error initializing blacklist: {await response.Content.ReadAsStringAsync()}");

            return Process(await response.Content.ReadAsStringAsync());
        }

        internal static Dictionary<string, HashSet<string>> Process(string blackList)
        {
            var result = new Dictionary<string, HashSet<string>>();
            foreach (var line in blackList.Split(new[] { '\r', '\n' }).Where(x => !string.IsNullOrWhiteSpace(x)))
            {
                var splittedLine = line.Split(' ');

                if (splittedLine.Length != 2) throw new Exception();

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
    }
}
