package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/17
 * @description: 通过func函数聚集RDD中的所有元素,先聚合分区内数据,再聚合分区间数据
 */
object Reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val makeRDD: RDD[Int] = sc.makeRDD(1 to 20, 3)
    val reduce1: Int = makeRDD.reduce(_ + _)
    println(reduce1)
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 2), ("c", 3), ("d", 4)))
    val reduce2: (String, Int) = rdd.reduce((x, y) => (x._1, x._2 + y._2))
    println(reduce2)
    sc.stop()
  }
}
