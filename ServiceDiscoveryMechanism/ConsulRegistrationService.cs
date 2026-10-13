using Consul;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class ConsulRegistrationService : IHostedService
{
    private readonly IConsulClient _consulClient;
    private readonly string _nodeId;
    private readonly int _nodePort;
    private readonly ILogger<ConsulRegistrationService> _logger;

    public ConsulRegistrationService(IConfiguration config, ILogger<ConsulRegistrationService> logger)
    {
        _logger = logger;
        var consulUrl = config["CONSUL_URL"] ?? "http://localhost:8500";

        _consulClient = new ConsulClient(config => config.Address = new Uri(consulUrl));
        _nodeId = config["NODE_ID"] ?? $"raft-node-{Guid.NewGuid()}";
        _nodePort = int.Parse(config["NODE_PORT"] ?? "5001");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var registration = new AgentServiceRegistration()
        {
            ID = _nodeId,
            Name = "raft-cluster",
            Address = _nodeId, // Registers the Docker container name for intra-cluster routing
            Port = _nodePort,
            Check = new AgentServiceCheck()
            {
                HTTP = $"http://{_nodeId}:{_nodePort}/health",
                Interval = TimeSpan.FromSeconds(5),
                Timeout = TimeSpan.FromSeconds(2),
                DeregisterCriticalServiceAfter = TimeSpan.FromSeconds(15),
            }
        };

        _logger.LogInformation("Registering node {NodeId} with Consul", _nodeId);
        await _consulClient.Agent.ServiceRegister(registration, cancellationToken);
    }    

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Deregistering node {NodeId} from Consul", _nodeId);
        await _consulClient.Agent.ServiceDeregister(_nodeId, cancellationToken);
    }
}