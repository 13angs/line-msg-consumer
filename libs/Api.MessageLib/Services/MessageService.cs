using Api.MessageLib.DTOs;
using Api.MessageLib.Interfaces;
using Api.MessageLib.Models;
using Api.MessageLib.Parameters;
using Api.MessageLib.Routes;
using Api.ReferenceLib.Exceptions;
using Api.ReferenceLib.Services;
using Api.ReferenceLib.Setttings;
using Api.ReferenceLib.Stores;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using Newtonsoft.Json;
using Simple.RabbitMQ;

namespace Api.MessageLib.Services
{
    public class MessageService : IMessageService
    {
        private readonly ILogger<MessageService> _logger;
        private readonly ILineMessageValidation _lineValidation;
        private readonly IHostEnvironment _env;
        private readonly IOptions<LineChannelSetting> _channelSetting;
        private readonly ISpecialKeywordHandler _skHandler;
        private readonly IMessageRepository _msgRepo;
        private readonly IMessagePublisher _publisher;
        public MessageService(ILogger<MessageService> logger, ILineMessageValidation lineValidation,
            IHostEnvironment env, IOptions<MongoConfigSetting> mongoConfig, IOptions<LineChannelSetting> channelSetting, ISpecialKeywordHandler skHandler, IMessageRepository msgRepo, IMessagePublisher publisher)
        {
            _logger = logger;
            _lineValidation = lineValidation;
            _env = env;
            _channelSetting = channelSetting;
            _skHandler = skHandler;
            _msgRepo = msgRepo;
            _publisher = publisher;
        }

        public async Task RetriveLineMessage(object content, string signature, string id)
        {
            // await Task.Yield();
            // _logger.LogInformation(JsonConvert.SerializeObject(content));

            // only calculate the signature in production mode
            if (!_env.IsDevelopment())
            {
                _lineValidation.Validate(signature, content);
            }

            string strContent = JsonConvert.SerializeObject(content);

            string groupId = string.Empty;
            string groupUserId = string.Empty;
            long timestamp = 0;

            BsonDocument msgDoc = BsonDocument.Parse(
                JsonConvert.SerializeObject(content)
            );

            BsonDocument firstEvent = msgDoc["events"][0]
                .ToBsonDocument();

            try
            {
                timestamp = Int64.Parse(firstEvent["timestamp"].ToString()!);
                // get the group id value using regex
                groupId = firstEvent["source"]["groupId"].ToString()!;

                // get the event type
                string eventType = firstEvent["type"].ToString()!;

                groupUserId = string.Empty;
                string messageType = string.Empty;

                // check if the event type=message
                if (eventType == LineEventTypes.Message)
                {
                    // get the line userId
                    groupUserId = firstEvent["source"]["userId"].ToString()!;
                }
                else if (eventType == LineEventTypes.MemberJoined)
                {
                    groupUserId = firstEvent["joined"]["members"][0]["userId"].ToString()!;
                }
            }
            // can not throw ask it will prevent verify in line console
            catch (ErrorResponseException ex)
            {
                _logger.LogWarning(ex.Description);
                return;
            }

            MessageModel messageModel = new MessageModel
            {
                ClientId = id,
                GroupId = groupId,
                GroupUserId = groupUserId,
                MessageType = MessageTypes.Receive,
                MessageObject = content,
                CreatedDate = TimezoneService.FromMili(timestamp).DateTime,
                ModifiedDate = TimezoneService.FromMili(timestamp).DateTime,
            };

            string messageModelStr = JsonConvert.SerializeObject(messageModel);
            MessageModel existingMessage = new MessageModel();

            // handle the special keywords
            // if true publish to the specific route key
            // then catch the error
            try
            {
                _skHandler.HandleGroupVerify(messageModelStr);
                existingMessage = await _msgRepo.FindMessageByGroupId(messageModel.GroupId);
                string webhookEventId = firstEvent["webhookEventId"].ToString()!;
                var existingMessages = _msgRepo.FindMessageByWebhookId(webhookEventId);
                if(existingMessages.Any())
                {
                    _logger.LogWarning("Message exist");
                    return;
                }
            }
            catch (ErrorResponseException ex)
            {
                // means the validating the group
                _logger.LogWarning(ex.Description);
                return;
            }

            messageModel.PlatformId = existingMessage.PlatformId;
            messageModelStr = JsonConvert.SerializeObject(messageModel);

            BsonDocument document = BsonDocument.Parse(
                messageModelStr
            );

            // await _messageCols.InsertOneAsync(document);
            await _msgRepo.AddMessage(messageModel);

            IDictionary<string, string> msgRoutingKeys = RoutingKeys.Message;
            string routingKey = msgRoutingKeys["create"];
            try
            {
                _publisher.Publish(messageModelStr, routingKey, null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }
            finally
            {
                _publisher.Dispose();
            }
            return;
        }

        public IEnumerable<MessageDto> GetMessages(MessageRoute route, MessageParam param)
        {
            var mo_type_format = new BsonArray
            {
                new BsonDocument("$eq", new BsonArray
                {
                    new BsonDocument("$first", "$message_object.events.message.type"),
                    "sticker"
                }),
                new BsonArray
                {
                    new BsonDocument
                    {
                        { "type", "sticker" },
                        { "id", new BsonDocument("$first", "$message_object.events.message.id") },
                        { "stickerId", new BsonDocument("$first", "$message_object.events.message.stickerId") },
                        { "packageId", new BsonDocument("$first", "$message_object.events.message.packageId") },
                        { "stickerResourceType", new BsonDocument("$first", "$message_object.events.message.stickerResourceType") },
                        { "static_url", new BsonDocument("$first", "$message_object.events.message.static_url") }
                    }
                },
                "$message_object.events.message"
            };

            var match_stage = new BsonDocument
            {
                { "platform_id", param.PlatformId },
                { "group_id", route.GroupId },
                { "message_object.events.message.type", new BsonDocument("$in", BsonArray.Create(param.GetMessageTypes()) ) }
            };

            if (!string.IsNullOrEmpty(param.Text))
            {
                match_stage["message_object.events.message.type"] = "text";
                match_stage["message_object.events.message.text"] = new BsonDocument("$regex", new BsonRegularExpression(param.Text, "i"));
            }
            if (!string.IsNullOrEmpty(param.StartDate))
            {
                match_stage["created_date"] = new BsonDocument("$regex", new BsonRegularExpression(param.StartDate, "i"));
            }

            var pipeline = new List<BsonDocument>
            {
                new BsonDocument("$sort", new BsonDocument(param.GetSortField(), param.GetSortValue())),
                new BsonDocument("$match", match_stage),
                new BsonDocument("$limit", param.Limit),
                new BsonDocument("$lookup",
                new BsonDocument
                {
                    { "from", "lmc_users" },
                    { "localField", "group_user_id" },
                    { "foreignField", "group_user_id" },
                    { "as", "users" }
                }),
                new BsonDocument("$addFields",
                    new BsonDocument
                    {
                        { "group_user_id", new BsonDocument("$first", "$users.group_user_id") },
                        { "display_name", new BsonDocument("$first", "$users.display_name") },
                        { "picture_url", new BsonDocument("$first", "$users.picture_url") },
                    }
                ),
                new BsonDocument("$project",
                    new BsonDocument
                    {
                        { "_id", 0 },
                        { "message_id", "$_id" },
                        { "group_id", 1 },
                        {
                            "user",
                            new BsonDocument
                            {
                                { "group_user_id", "$group_user_id" },
                                { "display_name", "$display_name" },
                                { "picture_url", "$picture_url" },
                            }
                        },
                        { "created_date", 1 },
                        { "modified_date", 1 },
                        {
                            "message_object",
                            new BsonDocument("$cond", mo_type_format)
                        }
                    }
                )
            };

            return _msgRepo.FindMessages<MessageDto>(pipeline);
        }
    }
}