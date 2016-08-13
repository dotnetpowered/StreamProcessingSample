using Kafka.Client.Cfg;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace StreamLoader
{
    class MessagePublisher
    {
        public static void Publish(Dictionary<string, string> kafkaParams, string PathToFile)
        {
            string[] brokerParts = kafkaParams["metadata.broker.list"].Split(':');
            var topicName = kafkaParams["topic"];
            var kafkaProducerConfig = new ProducerConfiguration(new List<BrokerConfiguration>
            {
                new BrokerConfiguration {Host = brokerParts[0], Port = int.Parse(brokerParts[1])}
            });
            var kafkaProducer = new Producer(kafkaProducerConfig);

            var json = JObject.Load(new JsonTextReader(new StreamReader(PathToFile)));

            var messageList = json.SelectToken("data").ToArray();
            foreach (var message in messageList)
            {
                var jsonText = message.ToString();
                try
                {
                    kafkaProducer.Send(new ProducerData<string, Message>(topicName, new Message(Encoding.UTF8.GetBytes(jsonText))));
                }
                catch (Exception exception)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{0} > Exception in message {1}", DateTime.Now, exception.Message);
                    Console.ResetColor();
                }
            }

            Console.WriteLine("Completed sending {0} messages", messageList.Count());
        }
    }
}
