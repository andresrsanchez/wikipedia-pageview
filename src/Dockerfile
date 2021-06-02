FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["pageview-processor-tests/pageview-processor-tests.csproj", "pageview-processor-tests/"]
COPY ["pageview-processor/pageview-processor.csproj", "pageview-processor/"]
COPY ["pageview-host/pageview-host.csproj", "pageview-host/"]
COPY ["./pageview.sln", "./pageview.sln"]
# RUN dotnet restore "pageview-host/pageview-host.csproj"
# RUN dotnet restore "pageview-processor-tests/pageview-processor-tests.csproj"
RUN dotnet restore "./pageview.sln"
COPY . .
WORKDIR /src
RUN dotnet build -c Release  --no-restore

FROM build AS test
WORKDIR "/src/pageview-processor-tests"
ENTRYPOINT ["dotnet", "test"]

FROM build AS publish
RUN dotnet publish -c release --no-build -o /app

FROM mcr.microsoft.com/dotnet/runtime:6.0
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "pageview-host.dll"]