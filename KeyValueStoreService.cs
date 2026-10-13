using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using RaftKVStore.Node.Protos;
using Google.Protobuf;

namespace RaftKVStore.Node
{
    public class KeyValueStoreService : KeyValueStore.KeyValueStoreBase
    {
        private readonly RaftStateManager _stateManager;
        private readonly ILogger<KeyValueStoreService> _logger;
        
        private static readonly ConcurrentDictionary<string, byte[]> _stateMachine = new();

        public KeyValueStoreService(RaftStateManager stateManager, ILogger<KeyValueStoreService> logger)
        {
            _stateManager = stateManager;
            _logger = logger;
        }

        public override Task<PutResponse> Put(PutRequest request, ServerCallContext context)
        {
            var response = new PutResponse { Success = false };

            if (_stateManager.CurrentRole != NodeRole.Leader)
            {
                _logger.LogInformation("Rejecting PUT request. Redirecting to Leader.");
                response.RedirectLeaderId = _stateManager.CurrentLeaderId ?? "";
                return Task.FromResult(response);
            }

            _stateMachine[request.Key] = request.Value.ToByteArray();
            _logger.LogInformation("Successfully committed Key: {Key}", request.Key);

            response.Success = true;
            return Task.FromResult(response);
        }

        public override Task<GetResponse> Get(GetRequest request, ServerCallContext context)
        {
            var response = new GetResponse { Found = false };

            if (_stateManager.CurrentRole != NodeRole.Leader)
            {
                response.RedirectLeaderId = _stateManager.CurrentLeaderId ?? "";
                return Task.FromResult(response);
            }

            if (_stateMachine.TryGetValue(request.Key, out byte[]? value))
            {
                response.Found = true;
                response.Value = ByteString.CopyFrom(value);
            }

            return Task.FromResult(response);
        }
    }
}