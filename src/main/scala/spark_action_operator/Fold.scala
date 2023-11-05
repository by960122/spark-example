package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description: 折叠操作,aggregate的简化操作,seqop和combop一样
 */
object Fold {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val foldRdd: RDD[Int] = sc.makeRDD(1 to 10, 2)
    println(foldRdd.fold(0)(_ + _))
    sc.stop()
  }
}
