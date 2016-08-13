using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using Newtonsoft.Json.Linq;

namespace StreamingProcessingHost
{
    class StreamProcessor
    { 
        public static void Process(string AppName, string CheckpointPath, Dictionary<string, string> kafkaParams)
        {
            var sparkContext = new SparkContext(new SparkConf().SetAppName(AppName));
            var topicList = new List<string> { kafkaParams["topic"] };
            var perTopicPartitionKafkaOffsets = new Dictionary<string, long>();
            const long slideDurationInMillis = 1000;

            StreamingContext sparkStreamingContext = StreamingContext.GetOrCreate(CheckpointPath,
                () =>
                {
                    var ssc = new StreamingContext(sparkContext, slideDurationInMillis);

                    var stream = KafkaUtils.CreateDirectStream(ssc, topicList, kafkaParams, perTopicPartitionKafkaOffsets);

                    stream.Map(kvp =>
                    {
                        if (kvp.Value != null)
                            return Encoding.UTF8.GetString(kvp.Value);
                        else
                            return null;
                    }
                    ).ForeachRDD(RDD =>
                    {
                        foreach (string line in RDD.Collect())
                        {
                            var message = JObject.Parse(line);
                            var _id = message.SelectToken("docid").ToString();
                            // =======================
                            //  TODO: Process message
                            // =======================
                        }
                    }
                    );
                    ssc.Checkpoint(CheckpointPath);

                    return ssc;
 
                });

            sparkStreamingContext.Start();
            sparkStreamingContext.AwaitTermination();
        }
    }
}
