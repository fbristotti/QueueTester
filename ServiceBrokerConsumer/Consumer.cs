using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBrokerConsumer
{
    public class Consumer : IDisposable
    {
        private SqlConnection _connection;

        public string QueueName { get; }

        public Consumer(string queueName)
        {
            if (string.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentException("message", nameof(queueName));
            }

            QueueName = queueName;
        }

        public void Dispose()
        {
            
        }

        public async Task ConsumeAsync(CancellationToken token)
        {
            using (var brokerRepository = new BrokerRepository(QueueName))
            {
                while (!token.IsCancellationRequested)
                {
                    var messages = await brokerRepository.NextMessagesAsync(token);

                    foreach (var message in messages)
                    {
                        if (message.MsgType != "DEFAULT")
                            continue;

                        await Task.Delay(1000); // simulate a heavy task
                        message.Message += ". Done!";
                        await brokerRepository.SendMessageAsync(message, token);
                        Console.WriteLine($"Received message: {message.Message}");
                    }
                    
                    await brokerRepository.EndConversationAsync(messages, token);
                    Console.WriteLine("end conversation");
                }
            }
        }
    }
}
