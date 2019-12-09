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
    public class ParallelConsumer : IDisposable
    {
        public string QueueName { get; }

        public ParallelConsumer(string queueName)
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
            using (var broker = new BrokerRepository(QueueName))
            {
                while (!token.IsCancellationRequested)
                {
                    var messages = await broker.NextMessagesAsync(token);
                    Task.Run(() => ProcessMessage(messages, token), token);
                }
            }
        }

        private void ProcessMessage(IList<BrokerMessage> messages, CancellationToken token)
        {
            var handle = messages.FirstOrDefault().Handle;
            Console.WriteLine($"processing handle={handle}...");

            using (var broker = new BrokerRepository(QueueName))
            {
                foreach (var message in messages)
                {
                    if (message.MsgType != "DEFAULT")
                        continue;

                    Console.WriteLine($"Received message: {message.Message}");
                    Thread.Sleep(5000); // simulate a heavy task
                    message.Message += ". Done!";
                    broker.SendMessageAsync(message, token).Wait();
                }
                
                broker.EndConversationAsync(messages, token).Wait();
            }
            Console.WriteLine($"processing handle={handle}... finished!");
        }
    }
}
