# wikipedia pageviews

The project show the top 25 pages on Wikipedia for each of the Wikipedia sub-domains.
## Usage
1. Clone the repo
   ```sh
   git clone https://github.com/andresrsanchez/wikipedia-pageview
   ```
2. Build and tag the image
   ```sh
   docker build -t wikidumps ./src
   ```
3. Map the volumes and run with dateFrom and dateTo options
   ```sh
   docker run --rm -v your-path:/app/cache -v your-other-path:/dumps wikidumps -df 20200101-000000 -dt 20200101-010000
   ```
## Design
The image above describe the process starting with none, one, or two inputs representing the range (from one date to another date). The image above shows the range between two dates ('DateFrom' - 'DateTo') and 'DatesToProcess' table represents a collection of parsed dates from string to date type.

![input](https://github.com/andresrsanchez/wikipedia-pageview/blob/main/images/input.PNG?raw=true)

The first thing to do is asking the cache (implemented with SQLite) if it contains the date, if the answser is yes then return the local path associated with this date, otherwise we will proceed to download and process the file of wikipedia pageviews. The download is executed in parallel with a maximum degree of parallelism of **three**, but because with a higher parallelism number we had issues with https://dumps.wikimedia.org/, receiving a lot of **503** responses status code so we need to test it more.

The download files are being saved without deleting them so select big sets of files carefully.

We store the results of this downloads in a ConcurrentBag, we migrate from BlockingCollection because there are features about this last data structure that we don't need, we 'only' need a **thread-safe** list.

![parallel](https://github.com/andresrsanchez/wikipedia-pageview/blob/main/images/parallel.PNG?raw=true)

So now we are going to talk about the processing of files, the goal is to get the top 25 pages by domain and subdomain ordered by number of views. So our main data structure is a **dictionary** to group each domain by key and order the titles by pageviews using a **priority queue**. I'm not convinced at all with the priorityqueue but we test other data structures like:

- SortedList/Treeset: We need a custom comparer to get titles in descending order and allow duplicate keys because the key is the number of page views. This custom comparer breaks 'Remove' and 'IndexOfkey' methods.
- SortedDictionary/Treemap: This structure makes no sense because the key needs to be the title not page views, and we need ordering by the last one.
- List: A simple list with (title, pageviews) item values, but in these case we need to maintain the list ordered ourselves and this is highly inneficient.

The lack of conviction with this data structure come from the needed of a copy to **invert** the whole priority queue to get it on descending order. Yes, we can make a custom comparator to get it automatically in descending order but we need to mantain the collection with **25** elements, so we need access to the **tail** to know whether to delete it or not.

![maindata](https://github.com/andresrsanchez/wikipedia-pageview/blob/main/images/maindata.PNG?raw=true)

Another data structure holds the blacklist in-memory, we made this with a dictionary to group each domain by key and
a hashet that contains the titles. We don't need something like a dictionary/hashmap because we don't have the page views by title in this file.

When we get the top 25 we persist it in filesystem and we return the collection of local paths with the results.

![return](https://github.com/andresrsanchez/wikipedia-pageview/blob/main/images/return.PNG?raw=true)

### Observability
We implement Application Insights (Azure monitor) because we already have account on Azure and implement on a .NET App takes literally 5 min. On the submitted app the key is not commited, we can see some results on the image above:

![insights](https://github.com/andresrsanchez/wikipedia-pageview/blob/main/images/insights.PNG?raw=true)


## Questions
### What additional things would you want to operate this application in a production setting?

Before going to a production environment i think that we need to know <u>the type of workloads that we are going to support</u> and what are the requirements in terms of performance and scalability. We think that the solution should not be the same if we have a daily workload or a yearly workload and we assume that we care about the resources that are being used.
Having said this, the first thing i would worry about is the **observability** because the solution only contains console logs and a very basic monitoring, with this we cannot move to production because we need more information about how my application is doing.
Another thing is the automatic retries on HTTP exceptions, now if the dumps server fails we capture the exception and that's it, maybe we can throw the exception and rely on the host and it's built-in retries strategy (like Azure Functions).

### What might change about your solution if this application needed to run automatically for each hour of the day?

- I would remove the parallel execution because we don't need it, we only have one to download one file per execution. 
- I would change the host because currently is a console app and we need some sort of automatic triggering so at this point we must know where are my app going to run, if the answer is the cloud we can implement an AWS Lambda/Azure function with a timer trigger but the ideal scenario for us would be a custom event based on a hook each time wikipedia upload a dump, but i think that this is not implemented on the server side. Otherwise if the anwser is on prem we can implement this with a service and a cron job, that triggers the application each hour.

    - In Windows the cron job is already implemented it on the service installation.
    - In Linux we can use systemd and cron jobs.

### How would you test this application?

Depends on the requirements for the application.

- We care about performance/scalability: In .NET we can implement performance tests with [BenchmarkDotnet](https://github.com/dotnet/BenchmarkDotNet) and the testing we must involve yearly workloads to check CPU, memory and disk I/O.
- We care about resilience: We would provoke all types of exceptions to test host resilience, these types of tests must be done in a dev environment (or local emulators but i wouldn't do that) to check error handling and possible automatic retries that are already implemented on the host (AWS Lambda for example).
- We care a little bit about all: I would implement a good set of tests with a monthly workload in a local environment and then i would test in a dev environment with a higher workload keeping an eye on the observability side. In local for example running from 20200101-000000 to 20200102-000000 and in dev from 20200101-000000 to 20210101-000000 and check the obtained metrics.

### How you’d improve on this application design?
- Implement a CD pipeline.
- Implement a graceful shutdown of the console project.
- Improve the value's data structure of the dictionary that holds the titles with it's pageviews, we cannot afford the invert of the whole priority queue to get the correct ordering (descending).
- If we care a lot about scalability: 
    - Remove the in-process paralell processing.
    - Do load testing against wikipedia's dumps server with JMeter for example, we need to know if the download part is the bottleneck on a producer/consumer app design.
    - Implement a producer/consumer architecture separating downloads and file processing. For example if the company already have implemented Kubernetes we can complicate everything with [this solution](https://kubernetes.io/docs/tasks/job/fine-parallel-processing-work-queue/), but we need some powerful nodes with good I/O.
    - Test our limits with the servers we would have in terms of memory and especially disk I/O because **we depend a lot (too much) on the filesystem**.
    - Replace SQLite with Redis for the caching side and address the persistence problem because this cache does not have TTL.
- If we don't want the producer/consumer solution another simple solution involves sequential processing and async streams, iterating through the collection of dates and processing them as soon as the download finish. **The actual solution is not efficient because the processing from the download stage to the sort stage is sequential**. 
- Implement a web app or an api to operate the application more easily.
- Improve (a lot) the observability side with a good set of metrics, traces and logs.






