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
            _connection?.Dispose();
            _receiveMessageCommand?.Dispose();
            _endConversationCommand?.Dispose();
        }

        protected async Task<SqlConnection> GetConnectionAsync(CancellationToken token)
        {
            if(_connection == null)
            {
                var connetionString = ConfigurationManager.ConnectionStrings["testdb"].ConnectionString;
                _connection = new SqlConnection(connetionString);
                await _connection.OpenAsync(token);
            }

            return _connection;
        }

        protected async Task EndConversationAsync(IList<Message> messages, CancellationToken token)
        {
            var handle = messages.FirstOrDefault()?.Handle;

            if (!handle.HasValue)
                return;

            var command = await GetEndCoversationCommand(token);
            command.Parameters["@handle"].Value = handle.Value;
            await command.ExecuteNonQueryAsync(token);
        }

        public async Task ConsumeAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var messages = await NextMessagesAsync(token);

                foreach(var message in messages)
                {
                    Console.WriteLine($"Received message: {message.Raw}");
                }

                await Task.Delay(2000);
                await EndConversationAsync(messages, token);
                Console.WriteLine("end conversation");
            }
        }

        private SqlCommand _receiveMessageCommand;
        private async Task<SqlCommand> GetReceiveMessageCommand(CancellationToken token)
        {
            if (_receiveMessageCommand == null)
            {
                var query = $"waitfor(receive conversation_handle, convert(varchar, message_body) from {QueueName}), timeout -1;";
                _receiveMessageCommand = new SqlCommand(query, await GetConnectionAsync(token))
                {
                    CommandTimeout = 0
                };
            }
            return _receiveMessageCommand;
        }

        private SqlCommand _endConversationCommand;
        private async Task<SqlCommand> GetEndCoversationCommand(CancellationToken token)
        {
            if (_endConversationCommand == null)
            {
                var query = "END CONVERSATION @handle";
                _endConversationCommand = new SqlCommand(query, await GetConnectionAsync(token));
                _endConversationCommand.Parameters.Add("@handle", System.Data.SqlDbType.UniqueIdentifier);
            }
            return _endConversationCommand;
        }

        private async Task<IList<Message>> NextMessagesAsync(CancellationToken token)
        {
            var command = await GetReceiveMessageCommand(token);
            var messages = new List<Message>();

            using (var reader = await command.ExecuteReaderAsync(token))
            {
                while (await reader.ReadAsync(token))
                {
                    messages.Add(new Message
                    {
                        Handle = (Guid)reader[0],
                        Raw = (string)reader[1]
                    });
                }
            }

            return messages;
        }

        public class Message
        {
            public Guid Handle { get; set; }
            public string Raw { get; set; }
        }
    }
}
