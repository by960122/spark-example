package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description: 在驱动程序中,以数组的形式返回数据集的所有元素
 */
object Collect {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    println(rdd.collect.mkString(","))
    sc.stop()
  }
}
