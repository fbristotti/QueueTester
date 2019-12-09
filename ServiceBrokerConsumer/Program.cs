using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBrokerConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var tokenSource = new CancellationTokenSource();
            
            var consumer = new Consumer("CSharpQueue");
            var task = consumer.ConsumeAsync(tokenSource.Token);

            Console.WriteLine("press q to exit");
            var command = "";
            while((command = Console.ReadLine())!= "q")
            {

            }

            tokenSource.Cancel();

            Console.WriteLine("bye");
        }
    }
}
