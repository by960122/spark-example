package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/15
 * @description: union整合rdd
 */
object Union {
  def main(args: Array[String]) {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val rdd1: RDD[Int] = sc.parallelize(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(5 to 10)
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
