using Api.CommonLib.DTOs;
using Api.CommonLib.Interfaces;
using Api.CommonLib.Models;
using Api.CommonLib.Setttings;
using Api.CommonLib.Stores;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;

namespace Api.CommonLib.RPCs
{
    public class MessageRpcClient : IMessageRpcClient
    {
        private readonly ICommonRpcClient _commonRpcClient;
        private readonly ILogger<MessageRpcClient> _logger;

        public MessageRpcClient(ICommonRpcClient commonRpcClient, IOptions<MongoConfigSetting> mongoConfig, ILogger<MessageRpcClient> logger)
        {
            _commonRpcClient = commonRpcClient;

            IMongoClient mongoClient = new MongoClient(mongoConfig.Value.HostName);
            IMongoDatabase mongodb = mongoClient.GetDatabase(mongoConfig.Value.DatabaseName);
            _logger = logger;
        }

        public Response AddUser(UserModel user)
        {
            CommonRpcRequest rpcRequest = new CommonRpcRequest{
                Action=RpcActions.Message["CreateUser"],
                Body=user
            };

            string request = JsonConvert.SerializeObject(rpcRequest);

            string strResponse = _commonRpcClient
                .SendRPCRequest(request, RpcQueueNames.Message);
            
            Response response = JsonConvert.DeserializeObject<Response>(strResponse)!;
            return response;
        }

        public LineChannelSetting GetChannel()
        {
            CommonRpcRequest rpcRequest = new CommonRpcRequest{
                Action=RpcActions.Message["GetChannel"]
            };

            string request = JsonConvert.SerializeObject(rpcRequest);

            string response = _commonRpcClient
                .SendRPCRequest(request, RpcQueueNames.Message);

            LineChannelSetting channelSetting = JsonConvert
                .DeserializeObject<LineChannelSetting>(response)!;
            
            return channelSetting;
        }
    }
}