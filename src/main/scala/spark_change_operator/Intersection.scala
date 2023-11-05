package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: 对源RDD和参数RDD求交集后返回一个新的RDD
 */
object Intersection {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val rdd1: RDD[Int] = sc.parallelize(1 to 7)
    val rdd2: RDD[Int] = sc.parallelize(5 to 10)
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
