using System;
using System.Collections.Generic;


namespace StreamLoader
{
    class Program
    {
        static void Main(string[] args)
        {            
            var kafkaParams = new Dictionary<string, string> //refer to http://kafka.apache.org/documentation.html#configuration
            {
                {"metadata.broker.list", "laptop-brianr:9092"},  // update with your machine name
                {"topic","SampleTopic2" }
            };

            var directory = new System.IO.DirectoryInfo(@".\sample_data");
            foreach (var file in directory.EnumerateFiles("*.json"))
                MessagePublisher.Publish(kafkaParams,file.FullName);
            Console.WriteLine("Finished");
            Console.ReadLine();
        }
    }
}
