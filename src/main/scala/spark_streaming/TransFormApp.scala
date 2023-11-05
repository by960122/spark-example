package spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description:
 * * 黑名单过滤
 * * 20180808,zs
 * * 20180808,ls
 * * 20180808,ww
 * * 黑名单列表
 * * zs
 * * ls
 */
object TransFormApp {
  def main(args: Array[String]): Unit = {
    val sc: SparkConf = new SparkConf().setMaster("local[3]").setAppName("NetWork").set("spark.testing.memory", "2147480000")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ssc = new StreamingContext(sc, Seconds(5))
    //      构建黑名单==>(zs,true)(ls,true)
    val blacks = List("zs", "ls")
    val BlakesRdd: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacks).map(x => (x, true))
    // 20180808,zs===》(zs:20180808,zs)
    val textStream: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
    val clickLog: DStream[String] = textStream.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(BlakesRdd).filter(x => x._2._2.getOrElse(false) != true).map({
        x => x._2._1
      })
    })
    clickLog.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
