package spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: SparkStream整合SparkSql完整词频统计
 */
object SocketSQL {
  def main(args: Array[String]): Unit = {
    val sc: SparkConf = new SparkConf().setMaster("local[3]").setAppName("NetWork").set("spark.testing.memory", "2147480000")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      val spark: SparkSession = new SparkSessionSingleton().getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordsDataFrame: DataFrame = rdd.map(w => Record(w)).toDF()
      wordsDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame: DataFrame = spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  case class Record(word: String)
}

class SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder.config(sparkConf).getOrCreate()
    }
    return instance
  }
}
