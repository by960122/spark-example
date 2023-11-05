package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: SparkStreamingContext 获取端口数据
 *               window执行cmd命令 nc -l -p 9999 发送数据
 */
object SocketReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStream").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // 从指定端口采集数据 bigdata是主机名
    val socketStreamLine: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
    val dStream: DStream[String] = socketStreamLine.flatMap(line => line.split(" "))
    // 将数据进行结构转变
    val map: DStream[(String, Int)] = dStream.map((_, 1)).reduceByKey(_ + _)
    // 结果打印
    map.print()
    // 启动采集器
    ssc.start()
    // 等待采集器执行,不能退出
    ssc.awaitTermination()
  }
}
