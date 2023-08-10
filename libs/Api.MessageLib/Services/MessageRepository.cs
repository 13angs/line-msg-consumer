using Api.MessageLib.Interfaces;
using Api.MessageLib.Models;
using Api.ReferenceLib.Exceptions;
using Api.ReferenceLib.Models;
using Api.ReferenceLib.Setttings;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;

namespace Api.MessageLib.Services
{
    public class MessageRepository : IMessageRepository
    {
        private readonly ILogger<MessageRepository> _logger;
        private readonly IMongoCollection<BsonDocument> _messageCols;
        private readonly IOptions<MongoConfigSetting> _mongoConfing;
        public MessageRepository(ILogger<MessageRepository> logger, IOptions<MongoConfigSetting> mongoConfing)
        {
            _logger = logger;
            _mongoConfing = mongoConfing;
            IMongoClient mongoClient = new MongoClient(_mongoConfing.Value.HostName);
            IMongoDatabase mongodb = mongoClient.GetDatabase(_mongoConfing.Value.DatabaseName);
            _messageCols = mongodb.GetCollection<BsonDocument>(MongoConfigSetting.Collections["Message"]);
        }

        private MessageModel HandleFindMessage(MessageModel messageModel, string id, string from)
        {
            if (messageModel != null)
            {
                return messageModel;
            }

            string resMsg = $"Message with {from} id {id} not found";
            _logger.LogError(resMsg);
            throw new ErrorResponseException(
                StatusCodes.Status404NotFound,
                resMsg,
                new List<Error>()
            );
        }

        public async Task<MessageModel> FindMessageByGroupId(string groupId)
        {
            MessageModel messageModel = await _messageCols
                .Find(x => x["_id"] == groupId)
                .As<MessageModel>()
                .FirstOrDefaultAsync();

            return HandleFindMessage(messageModel, groupId, "group");
        }

        public IEnumerable<MessageModel> FindMessages(List<BsonDocument> pipeline)
        {
            return _messageCols
                .Aggregate<MessageModel>(pipeline.ToArray())
                .ToEnumerable();
        }

        public async Task<Response> AddMessage(MessageModel model)
        {
            string resMsg = "Successfully save the message";
            BsonDocument document = BsonDocument.Parse(
                JsonConvert.SerializeObject(model)
            );

            await _messageCols.InsertOneAsync(document);

            return new Response{
                StatusCode=StatusCodes.Status200OK,
                Message=resMsg
            };
        }
    }
}