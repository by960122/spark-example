package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.DriverManager

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: 获取到的数据保存到数据库
 */
object TextFileReceiverSave {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 如果使用了stateful的算子,必须要设置checkpoint
    // 在生产环境中,建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("hadoop000", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val state: DStream[(String, Int)] = result.updateStateByKey[Int](updateFunction _)
    state.print()
    //    将结果写到mysql,下面注释的代码会报序列化异常
    //    result.foreachRDD { rdd =>
    //      val connection = createConn() // executed at the driver
    //      rdd.foreach { record =>
    //        val sql = "insert into wordcount(word,wordcount) values ('" + record._1 + "'," + record._2 + ")" // executed at the worker
    //        connection.createStatement().execute(sql)
    //      }
    //    }
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOnRecords => {
        val connection = createConn() // executed at the driver
        partitionOnRecords.foreach(record => {
          val sql = "insert into wordcount(word,wordcount) values ('" + record._1 + "'," + record._2 + ")" // executed at the worker
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def createConn() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "admin")
  }

  /**
   * 把当前的数据去更新已有的或者是老的数据
   *
   * @param currentValues 当前的
   * @param preValues     老的
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }
}
