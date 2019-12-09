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

        public async Task ConsumeAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var messages = await NextMessagesAsync(token);

                foreach (var message in messages)
                {
                    if (message.MsgType != "DEFAULT")
                        continue;

                    await Task.Delay(1000); // simulate a heavy task

                    message.Raw += ". Done!";
                    Console.WriteLine($"Received message: {message.Raw}");
                }

                await SendMessages(messages, token);
                await EndConversationAsync(messages, token);
                Console.WriteLine("end conversation");
            }
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
                        Raw = (string)reader[1],
                        MsgType = (string)reader[2]
                    });
                }
            }

            return messages;
        }
        private async Task SendMessages(IList<Message> messages, CancellationToken token)
        {
            var command = await GetSendMessageCommand(token);

            foreach(var message in messages)
            {
                command.Parameters["@handle"].Value = message.Handle;
                command.Parameters["@message"].Value = message.Raw;
                await command.ExecuteNonQueryAsync(token);
            }
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
        protected async Task<SqlConnection> GetConnectionAsync(CancellationToken token)
        {
            if (_connection == null)
            {
                var connetionString = ConfigurationManager.ConnectionStrings["testdb"].ConnectionString;
                _connection = new SqlConnection(connetionString);
                await _connection.OpenAsync(token);
            }

            return _connection;
        }

        private SqlCommand _receiveMessageCommand;
        private async Task<SqlCommand> GetReceiveMessageCommand(CancellationToken token)
        {
            if (_receiveMessageCommand == null)
            {
                var query = $"waitfor(receive conversation_handle, convert(varchar, message_body), message_type_name from {QueueName}), timeout -1;";
                _receiveMessageCommand = new SqlCommand(query, await GetConnectionAsync(token))
                {
                    CommandTimeout = 0
                };
            }
            return _receiveMessageCommand;
        }

        private SqlCommand _sendMessageCommand;
        private async Task<SqlCommand> GetSendMessageCommand(CancellationToken token)
        {
            if (_sendMessageCommand == null)
            {
                var query = $"SEND ON CONVERSATION @handle(@message)";

                _sendMessageCommand = new SqlCommand(query, await GetConnectionAsync(token));
                _sendMessageCommand.Parameters.AddRange(new[]
                {
                    new SqlParameter("@handle", System.Data.SqlDbType.UniqueIdentifier),
                    new SqlParameter("@message", System.Data.SqlDbType.VarChar, -1)
                });
            }
            return _sendMessageCommand;
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
        public class Message
        {
            public Guid Handle { get; set; }
            public string Raw { get; set; }
            public string MsgType { get; set; }
        }
    }
}
