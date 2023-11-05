package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/14
 * @description: 按照指定的规则过滤
 */
object Filter {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val fil: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = fil.filter(_ % 2 == 1)
    filterRDD.collect().foreach(println)
    sc.stop()
  }
}
