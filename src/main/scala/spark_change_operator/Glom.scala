package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/16
 * @description: 将每一个分区形成一个数组,形成新的RDD类型时RDD[Array[T]]
 */
object Glom {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val makeRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
    val glomRDD: RDD[Array[Int]] = makeRDD.glom()
    glomRDD.collect().foreach(array1 => {
      println(array1.mkString(","))
    })
    println("求最大值: ")
    glomRDD.collect().foreach(array => {
      println(array.max)
    })
    sc.stop()
  }
}
