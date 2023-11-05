package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description: aggregate(初始值)(分区内操作, 分区间操作)
 */
object Aggregate {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    var rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)
    println(rdd1.aggregate(0)(_ + _, _ + _))
    sc.stop()
  }
}
