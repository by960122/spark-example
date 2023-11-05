package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description:
 * 无状态的转化操作:每一个时间段是独立的,
 * 有状态的转化操作:时间段的结果累加
 */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("UpdateStateByKey").set("spark.testing.memory", "2147480000")
    val streamContext = new StreamingContext(sparkConf, Seconds(10))
    streamContext.checkpoint("hdfs://192.168.1.101:9000/spark-streaming")
    val socketStreamLine: ReceiverInputDStream[String] = streamContext.socketTextStream("192.168.1.101", 9999)
    val result: DStream[(String, Int)] = socketStreamLine.flatMap(_.split(" ")).map((_, 1))
    //    无状态
    //    result.reduceByKey(_ + _).print()
    //    有状态
    val value: DStream[(String, Int)] =
    result.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    value.print()
    //    result.updateStateByKey[Int](updateFunction _).print()

    streamContext.start()
    streamContext.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }
}
