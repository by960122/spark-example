package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: SparkStreamingContext 获取文件数据
 */
object TextFileReceiver {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStream").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val fileStreamLine: DStream[String] = ssc.textFileStream("C:\\WorkSpace\\spark_example\\spark-warehouse\\")
    //    val fileStreamLine: DStream[String] = ssc.textFileStream("hdfs://192.168.181.128:8020/spark/")
    //将采集数据进行分解
    val result: DStream[(String, Int)] = fileStreamLine.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
