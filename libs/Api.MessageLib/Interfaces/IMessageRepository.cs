using Api.MessageLib.Models;
using Api.ReferenceLib.Models;
using MongoDB.Bson;

namespace Api.MessageLib.Interfaces
{
    public interface IMessageRepository
    {
        public IEnumerable<T> FindMessages<T>(List<BsonDocument> pipeline);
        public Task<MessageModel> FindMessageByGroupId(string groupId);
        public IEnumerable<MessageModel> FindMessageByWebhookId(string webhookId);
        public Task<Response> AddMessage(MessageModel model);
        public Task<Response> ReplaceMessage(BsonDocument document);
    }
}