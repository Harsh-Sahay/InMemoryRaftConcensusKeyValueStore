using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using RaftKVStore.Node.Protos;

namespace RaftKVStore.Node
{
    public class RaftGrpcService : RaftConsensus.RaftConsensusBase
    {
        private readonly RaftStateManager _stateManager;
        private readonly ILogger<RaftGrpcService> _logger;

        public RaftGrpcService(RaftStateManager stateManager, ILogger<RaftGrpcService> logger)
        {
            _stateManager = stateManager;
            _logger = logger;
        }

        public override Task<VoteResponse> RequestVote(VoteRequest request, ServerCallContext context)
        {
            var response = new VoteResponse { Term = _stateManager.CurrentTerm, VoteGranted = false };

            if (request.Term < _stateManager.CurrentTerm)
            {
                return Task.FromResult(response);
            }

            if (request.Term > _stateManager.CurrentTerm)
            {
                _stateManager.TransitionToFollower(request.Term);
            }

            if (_stateManager.VotedFor == null || _stateManager.VotedFor == request.CandidateId)
            {
                _stateManager.GrantVote(request.CandidateId);
                response.VoteGranted = true;
                _logger.LogInformation("Granted vote to Candidate {CandidateId} for Term {Term}", request.CandidateId, request.Term);
            }

            response.Term = _stateManager.CurrentTerm;
            return Task.FromResult(response);
        }

        public override Task<AppendResponse> AppendEntries(AppendRequest request, ServerCallContext context)
        {
            var response = new AppendResponse { Term = _stateManager.CurrentTerm, Success = false };

            if (request.Term < _stateManager.CurrentTerm)
            {
                return Task.FromResult(response);
            }

            _stateManager.ReceiveHeartbeat();

            if (request.Term > _stateManager.CurrentTerm || _stateManager.CurrentRole != NodeRole.Follower)
            {
                _stateManager.TransitionToFollower(request.Term);
                _stateManager.SetCurrentLeader(request.LeaderId); 
            }

            response.Success = true;
            response.Term = _stateManager.CurrentTerm;
            return Task.FromResult(response);
        }
    }
}