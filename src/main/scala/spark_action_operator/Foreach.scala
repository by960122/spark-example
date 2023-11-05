package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description: 在数据集的每一个元素上,运行函数func进行更新
 */
object Foreach {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    var foreachRdd: RDD[Int] = sc.makeRDD(1 to 5, 2)
    foreachRdd.foreach(println(_))
    sc.stop()
  }
}
