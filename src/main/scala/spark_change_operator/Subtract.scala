package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/15
 * @description: 计算差的一种函数,去除两个RDD中相同的元素,不同的RDD将保留下来
 */
object Subtract {
  def main(args: Array[String]) {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val rdd1: RDD[Int] = sc.parallelize(3 to 8)
    val rdd2: RDD[Int] = sc.parallelize(1 to 5)
    rdd1.subtract(rdd2).collect().foreach(println)
    sc.stop()
  }
}
