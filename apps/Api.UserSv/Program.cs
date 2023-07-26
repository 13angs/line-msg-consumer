using Api.CommonLib.Exceptions;
using Api.CommonLib.Interfaces;
using Api.CommonLib.RPCs;
using Api.CommonLib.Services;
using Api.CommonLib.Setttings;
using Api.UserLib.Interfaces;
using Api.UserLib.Services;
using Api.UserSv.HostedServices;
using Newtonsoft.Json.Serialization;
using RabbitMQ.Client;
using Simple.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);
IConfiguration configuration = builder.Configuration;

// Add services to the container.

// configure controller to use Newtonsoft as a default serializer
builder.Services.AddControllers()
    .AddNewtonsoftJson(options =>
        options.SerializerSettings.ReferenceLoopHandling = Newtonsoft
            .Json.ReferenceLoopHandling.Ignore)
                .AddNewtonsoftJson(options => options.SerializerSettings.ContractResolver
                    = new DefaultContractResolver()
);
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IBasicConnection>(new BasicConnection(configuration["RabbitMQConfig:HostName"], true));
// Register message subscriber
builder.Services.AddSingleton<IMessageSubscriber>(x =>
    new MessageSubscriber(
        x.GetRequiredService<IBasicConnection>(),
        configuration["RabbitMQConfig:ExchangeName"], // exhange name
        configuration["RabbitMQConfig:QueueName"], // queue name
        configuration["RabbitMQConfig:RoutingKey"], // routing key
        ExchangeType.Topic, // exchange type
        autoAck: bool.Parse(configuration["RabbitMQConfig:Properties:AutoAck"]),
        prefetchCount: ushort.Parse(configuration["RabbitMQConfig:Properties:PrefetchCount"])
    ));
builder.Services.AddScoped<IMessagePublisher>(x =>
    new MessagePublisher(
        x.GetRequiredService<IBasicConnection>(),
        configuration["RabbitMQConfig:ExchangeName"], // exhange name
        ExchangeType.Topic // exchange type
    ));
builder.Services.AddHostedService<UserDataCollector>();
builder.Services.AddSingleton<IMessageConsumer, UserMessageConsumerService>();
builder.Services.AddSingleton<IMessageRpcClient, MessageRpcClient>();
builder.Services.AddSingleton<ICommonRpcClient, CommonRpcClient>();
builder.Services.AddSingleton<ILineUserInfo, LineUserInfoService>();
builder.Services.Configure<RabbitmqConfigSetting>(configuration.GetSection("RabbitMQConfig"));
builder.Services.Configure<MongoConfigSetting>(configuration.GetSection("MongoConfig"));
builder.Services.AddSingleton<IUserRepository, UserRepository>();


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}else{
    app.UseResponseExceptionHandler();
}

// app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
