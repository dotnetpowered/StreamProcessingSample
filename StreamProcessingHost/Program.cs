using System.Collections.Generic;

namespace StreamingProcessingHost
{
    class Program
    {
        static void Main(string[] args)
        {
            // ========================================================
            // TODO: Set based on your machine name and paths
            // ========================================================
            var kafkaParams = new Dictionary<string, string> //refer to http://kafka.apache.org/documentation.html#configuration
            {
                {"metadata.broker.list", "laptop-brianr:9092"},
                {"auto.offset.reset", "smallest"},
                {"topic","SampleTopic2" }
            };
            StreamProcessor.Process("Sample Streaming App2", "file:///c:/tmp/hive/checkpoint2.out" ,kafkaParams);
        }
    }
}
