using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using pageview_processor;
using Serilog;
using Serilog.Events;
using System;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    class Program
    {
        static async Task Main(string[] args) => await CommandLineApplication.ExecuteAsync<Program>(args);

        async Task OnExecuteAsync()
        {
            var services = new ServiceCollection()
                .AddHttpClient()
                .AddLogging(builder =>
                {
                    builder.ClearProviders();
                    builder.AddSerilog(new LoggerConfiguration()
                        .MinimumLevel.Information()
                        .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                        .MinimumLevel.Override("System", LogEventLevel.Warning)
                        .WriteTo.Console()
                        .CreateLogger(), dispose: true);
                })
                .AddTransient<WikipediaDumpsProcessor>()
                .BuildServiceProvider();

            try
            {
                var processor = services.GetRequiredService<WikipediaDumpsProcessor>();
                await processor.ProcessAndGetResultsFilePath(DateFrom, DateTo);
            }
            catch (Exception ex)
            {
                var logger = services.GetRequiredService<ILogger<Program>>();
                logger.LogError(ex, "An error occurred.");
            }
        }

        [Option(LongName = "dateFrom", ShortName = "df")]
        public string DateFrom { get; }
        [Option(LongName = "dateTo", ShortName = "dt")]
        public string DateTo { get; }
    }
}
