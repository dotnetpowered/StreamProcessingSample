using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Messages;
using Kafka.Client.Serialization;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace StreamConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            // ========================================================
            // TODO: Set based on your machine name and topics
            // ========================================================
            BalancedConsumer("ConsumerGroup", "Consumer1", "SampleTopic2", 1, "localhost:2181", message =>
                {
                    string msg = Encoding.UTF8.GetString(message.Payload);
                    Console.WriteLine(msg);
                    Console.ReadLine();
                });
        }

        static void BalancedConsumer(string consumerGroupId, string uniqueConsumerId, string topic, int threads, string zookeeperServer, Action<Message> ProcessMessage)
        {
            // Here we create a balanced consumer on one consumer machine for consumerGroupId. All machines consuming for this group will get balanced together
            ConsumerConfiguration config = new ConsumerConfiguration
            {
                AutoCommit = false,
                GroupId = consumerGroupId,
                ConsumerId = uniqueConsumerId,
                ZooKeeper = new ZooKeeperConfiguration(zookeeperServer, 30000, 30000, 2000)
            };
            var balancedConsumer = new ZookeeperConsumerConnector(config, true);

            // grab streams for desired topics 
            var topicMap = new Dictionary<string, int>() { { topic, threads } };
            var streams = balancedConsumer.CreateMessageStreams(topicMap, new DefaultDecoder());
            var KafkaMessageStream = streams[topic][0];

            // start consuming stream
            foreach (Message message in KafkaMessageStream.GetCancellable(new CancellationToken()))
            {
                ProcessMessage(message);
                balancedConsumer.CommitOffsets();
            }
        }
    }
}
