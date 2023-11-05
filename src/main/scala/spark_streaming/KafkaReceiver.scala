package spark_streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: SparkStream 通过 Receiver 操作 Kafka
 */
object KafkaReceiver {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStream").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //    Receiver方式: 可以直接写topic "zhaogw"
    //    val kafkaDsream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "192.168.1.101:2181", "zhaogw", Map("zhaogw" -> 3))
    //    Direct方式:

    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.1.201:9092,192.168.1.202:9092,192.168.1.203:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "group1",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000",
      // lastest
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaDsream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("zhaogw"), kafkaParams))
    //    窗口大小,滑动步长都应该是采集周期的整数倍
    kafkaDsream.window(Seconds(3), Seconds(3))
    val result: DStream[(String, Int)] = kafkaDsream.flatMap(t => t.key().toString.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
