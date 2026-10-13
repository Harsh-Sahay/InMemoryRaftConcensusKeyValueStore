FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 5001 5002 5003

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["RaftKVStore.Node.csproj", "./"]
RUN dotnet restore "RaftKVStore.Node.csproj"
COPY . .
RUN dotnet build "RaftKVStore.Node.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "RaftKVStore.Node.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "RaftKVStore.Node.dll"]