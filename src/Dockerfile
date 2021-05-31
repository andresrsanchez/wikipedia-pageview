FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["pageview-processor-tests/pageview-processor-tests.csproj", "pageview-processor-tests/"]
COPY ["pageview-processor/pageview-processor.csproj", "pageview-processor/"]
COPY ["pageview-host/pageview-host.csproj", "pageview-host/"]
RUN dotnet restore "pageview-host/pageview-host.csproj"
RUN dotnet restore "pageview-processor-tests/pageview-processor-tests.csproj"
COPY . .
#WORKDIR "/src/pageview-processor-tests"
WORKDIR /src
# RUN dotnet build -c Release  --no-restore
RUN dotnet build -c Debug  --no-restore

FROM build AS test
WORKDIR "/src/pageview-processor-tests"
ENTRYPOINT ["dotnet", "test"]