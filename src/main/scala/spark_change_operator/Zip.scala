package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/15
 * @description: 将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量必须相同,否则会抛出异常
 */
object Zip {
  def main(args: Array[String]) {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3), 3)
    val rdd2: RDD[String] = sc.parallelize(Array("a", "b", "c"), 3)
    rdd1.zip(rdd2).collect().foreach(println)
    rdd2.zip(rdd1).collect().foreach(println)
    //    val rdd3 = sc.parallelize(Array("a","b","c"),2)
    //    rdd1.zip(rdd3).collect().foreach(println)
    sc.stop()
  }
}
