using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace kafka_stopwatch
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var kafkaConfig = new Dictionary<string, object>
            {
                {"bootstrap.servers", "localhost:9092"},    
                { "group.id", Guid.NewGuid() },
                { "auto.offset.reset", "smallest" }
            };

            var cts = new CancellationTokenSource();
            var ct = cts.Token;
            var pTask = Task.Run(() =>
            {
                using (var kp = new Producer<Null, string>(
                    kafkaConfig,
                    new NullSerializer(),
                    new StringSerializer(Encoding.UTF8)))
                {
                    //do nothing for a moment
                }
            }, ct);

            var cTask = Task.Run(() =>
            {
                using (var kc = new Consumer<Null, string>(
                    kafkaConfig,
                    new NullDeserializer(),
                    new StringDeserializer(Encoding.UTF8)))
                {
                    kc.Subscribe("hello-topic");
                    while (true)
                    {
                        if (!kc.Consume(out var msg, 100))
                        {
                            break;
                        }                        
                        Console.WriteLine(msg.Value);
                    }
                }
            }, ct);
            Task.WaitAll(cTask, pTask);
            Console.WriteLine("all the castles are conqured and the dragons are banished");
        }
    }
}