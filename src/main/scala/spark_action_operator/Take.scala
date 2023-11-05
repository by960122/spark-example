package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description: 返回一个由RDD的前n个元素组成的数组
 */
object Take {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(2, 5, 4, 6, 8, 3))
    println("take: " + rdd.take(3).mkString(","))
    println("takeOrdered: " + rdd.takeOrdered(3).mkString(","))
    sc.stop()
  }
}
