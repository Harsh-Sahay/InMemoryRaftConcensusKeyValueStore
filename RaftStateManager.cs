using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Consul;

namespace RaftKVStore.Node
{
    public enum NodeRole
    {
        Follower,
        Candidate,
        Leader
    }

    public class RaftStateManager
    {
        private readonly object _stateLock = new object();
        private readonly string _currentNodeId;
        private readonly string _consulUrl;

        public int CurrentTerm { get; private set; } = 0;
        public string? VotedFor { get; private set; } = null;
        public NodeRole CurrentRole { get; private set; } = NodeRole.Follower;
        public string? CurrentLeaderId { get; private set; } = null;
        public string NodeId => _currentNodeId;

        private TaskCompletionSource<bool> _heartbeatTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public RaftStateManager(IConfiguration config)
        {
            _currentNodeId = config["NODE_ID"] ?? $"node-{Guid.NewGuid()}";
            _consulUrl = config["CONSUL_URL"] ?? "http://localhost:8500";
        }

        public void TransitionToCandidate()
        {
            lock (_stateLock)
            {
                CurrentRole = NodeRole.Candidate;
                CurrentTerm++;
                VotedFor = _currentNodeId;
                CurrentLeaderId = null;
            }
        }

        public void TransitionToLeader()
        {
            lock (_stateLock)
            {
                CurrentRole = NodeRole.Leader;
                CurrentLeaderId = _currentNodeId;
            }
        }

        public void TransitionToFollower(int newTerm)
        {
            lock (_stateLock)
            {
                CurrentRole = NodeRole.Follower;
                CurrentTerm = newTerm;
                VotedFor = null;
            }
        }

        public void SetCurrentLeader(string leaderId)
        {
            lock (_stateLock)
            {
                CurrentLeaderId = leaderId;
            }
        }

        public void GrantVote(string candidateId)
        {
            lock (_stateLock)
            {
                VotedFor = candidateId;
            }
        }

        public void ReceiveHeartbeat()
        {
            lock (_stateLock)
            {
                _heartbeatTcs.TrySetResult(true);
                _heartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public async Task<bool> WaitForHeartbeatAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            TaskCompletionSource<bool> currentTcs;
            lock (_stateLock)
            {
                currentTcs = _heartbeatTcs;
            }

            var timeoutTask = Task.Delay(timeout, cancellationToken);
            var completedTask = await Task.WhenAny(currentTcs.Task, timeoutTask);

            return completedTask == currentTcs.Task;
        }

        public async Task<List<string>> GetPeerAddressesAsync()
        {
            using var client = new ConsulClient(config => config.Address = new Uri(_consulUrl));
            var queryResult = await client.Health.Service("raft-cluster", tag: string.Empty, passingOnly: true);

            var peerAddresses = new List<string>();
            foreach (var service in queryResult.Response)
            {
                if (service.Service.ID != _currentNodeId)
                {
                    peerAddresses.Add($"http://{service.Service.Address}:{service.Service.Port}");
                }
            }

            return peerAddresses;
        }
    }
}