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
        static async Task Main(string[] args)
        {
            var builder = new HostBuilder()
                .ConfigureLogging((context, builder) =>
                {
                    var logger = new LoggerConfiguration()
                        .MinimumLevel.Information()
                        .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                        .MinimumLevel.Override("System", LogEventLevel.Warning)
                        .WriteTo.Console()
                        .CreateLogger();

                    Log.Logger = logger;

                    builder.ClearProviders();
                    builder.AddSerilog(logger, dispose: true);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHttpClient();
                    services.AddTransient<WikipediaDumpsProcessor>();
                }).UseConsoleLifetime();

            var host = builder.Build();

            try
            {
                var myService = host.Services.GetRequiredService<WikipediaDumpsProcessor>();
                await myService.ProcessAndGetResultsFilePath("20200101-000000", "20200101-090000");
            }
            catch (Exception ex)
            {
                var logger = host.Services.GetRequiredService<ILogger<Program>>();

                logger.LogError(ex, "An error occurred.");
            }
        }
    }
}
