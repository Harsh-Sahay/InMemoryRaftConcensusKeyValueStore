using RaftKVStore.Node;
using Microsoft.AspNetCore.Server.Kestrel.Core;

// CRITICAL: This allows the .NET gRPC client to send HTTP/2 traffic over unencrypted http:// connections
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = WebApplication.CreateBuilder(args);

// Explicitly tell Kestrel to accept both HTTP/1.1 and HTTP/2 on unencrypted ports
builder.WebHost.ConfigureKestrel(options =>
{
    options.ConfigureEndpointDefaults(listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
    });
});

builder.Services.AddGrpc();
builder.Services.AddSingleton<RaftStateManager>();
builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<ConsulRegistrationService>();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.MapGrpcService<RaftGrpcService>();
app.MapGrpcService<KeyValueStoreService>();
app.MapHealthChecks("/health");

app.Run();