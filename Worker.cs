using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RaftKVStore.Node.Protos;

namespace RaftKVStore.Node
{
    public class Worker : BackgroundService
    {
        private readonly RaftStateManager _stateManager;
        private readonly ILogger<Worker> _logger;
        private readonly Random _random = new();
        
        // CACHE gRPC clients! Re-establishing an HTTP/2 channel takes ~100-200ms.
        // Reusing the channel multiplexes all RPCs over a single persistent connection.
        private readonly ConcurrentDictionary<string, RaftConsensus.RaftConsensusClient> _clients = new();

        public Worker(RaftStateManager stateManager, ILogger<Worker> logger)
        {
            _stateManager = stateManager;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var timeout = GetRandomizedElectionTimeout();
                bool heartbeatReceived = await _stateManager.WaitForHeartbeatAsync(timeout, stoppingToken);

                if (!heartbeatReceived && _stateManager.CurrentRole != NodeRole.Leader)
                {
                    _logger.LogInformation("Election timeout expired. Transitioning to Candidate.");
                    _stateManager.TransitionToCandidate();
                    await InitiateElectionAsync(stoppingToken);
                }
            }
        }

        private TimeSpan GetRandomizedElectionTimeout()
        {
            // Bumped slightly to 300-600ms to prevent clumped elections under heavy Docker load
            return TimeSpan.FromMilliseconds(_random.Next(300, 601)); 
        }

        private RaftConsensus.RaftConsensusClient GetClient(string peerUrl)
        {
            return _clients.GetOrAdd(peerUrl, url => 
            {
                var handler = new SocketsHttpHandler
                {
                    PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                    KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                    EnableMultipleHttp2Connections = true
                };

                var channel = GrpcChannel.ForAddress(url, new GrpcChannelOptions 
                { 
                    HttpHandler = handler,
                    Credentials = ChannelCredentials.Insecure // Explicitly bypass TLS requirements
                });
                
                return new RaftConsensus.RaftConsensusClient(channel);
            });
        }

        private async Task InitiateElectionAsync(CancellationToken stoppingToken)
        {
            var peers = await _stateManager.GetPeerAddressesAsync();
            _logger.LogInformation("Broadcasting RequestVote to {Count} peer nodes", peers.Count);

            if (peers.Count == 0) return;

            int currentTerm = _stateManager.CurrentTerm;
            int votesReceived = 1; // Candidate always votes for itself
            int quorum = (peers.Count + 1) / 2 + 1;

            var voteTasks = peers.Select(async peerUrl =>
            {
                try
                {
                    var client = GetClient(peerUrl);
                    var request = new VoteRequest
                    {
                        Term = currentTerm,
                        CandidateId = _stateManager.NodeId,
                        LastLogIndex = 0,
                        LastLogTerm = 0
                    };

                    // Send vote request with a more forgiving deadline for cross-container latency
                    var response = await client.RequestVoteAsync(request, deadline: DateTime.UtcNow.AddMilliseconds(500));

                    if (response.VoteGranted)
                    {
                        Interlocked.Increment(ref votesReceived);
                    }
                    else if (response.Term > currentTerm)
                    {
                        _stateManager.TransitionToFollower(response.Term);
                    }
                }
                catch (RpcException ex)
                {
                    _logger.LogWarning("Failed to request vote from {PeerUrl}: {Status}", peerUrl, ex.StatusCode);
                }
                catch (Exception ex)
                {
                     _logger.LogWarning("Network failure to {PeerUrl}: {Message}", peerUrl, ex.Message);
                }
            });

            await Task.WhenAll(voteTasks);

            if (_stateManager.CurrentRole == NodeRole.Candidate && votesReceived >= quorum)
            {
                _logger.LogInformation("Quorum reached ({Votes}/{TotalNodes}). Transitioning to LEADER.", votesReceived, peers.Count + 1);
                _stateManager.TransitionToLeader();
                
                // Fire and forget the heartbeat loop
                _ = BroadcastHeartbeatsAsync(stoppingToken);
            }
        }

        private async Task BroadcastHeartbeatsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested && _stateManager.CurrentRole == NodeRole.Leader)
            {
                var peers = await _stateManager.GetPeerAddressesAsync();
                var currentTerm = _stateManager.CurrentTerm;
                var leaderId = _stateManager.NodeId;

                var heartbeatTasks = peers.Select(async peerUrl =>
                {
                    try
                    {
                        var client = GetClient(peerUrl);
                        var request = new AppendRequest
                        {
                            Term = currentTerm,
                            LeaderId = leaderId,
                            PrevLogIndex = 0,
                            PrevLogTerm = 0,
                            LeaderCommit = 0
                        };

                        var response = await client.AppendEntriesAsync(request, deadline: DateTime.UtcNow.AddMilliseconds(200));
                        
                        if (response.Term > currentTerm)
                        {
                            _logger.LogInformation("Found higher term {Term}. Stepping down from Leader.", response.Term);
                            _stateManager.TransitionToFollower(response.Term);
                        }
                    }
                    catch (Exception)
                    {
                        // Heartbeats fail silently if follower goes offline to prevent log spam
                    }
                });

                await Task.WhenAll(heartbeatTasks);
                await Task.Delay(100, stoppingToken); 
            }
        }
    }
}