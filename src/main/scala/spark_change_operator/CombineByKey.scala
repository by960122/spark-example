package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/16
 * @description: 需求:创建一个pairRDD,计算相同key对应值的平均值
 *               CombineByKey: 分区内=(标记+累加) 分区间:累加
 */
object CombineByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val input: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
    val combine: RDD[(String, (Int, Int))] = input.combineByKey((_, 1), // 分区内:标记 (88,1)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // 分区内:相加 (88,1) + 91 => ((88 + 91),1+1)
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) // 分区间: 累加
    combine.collect().foreach(println)
    val result: RDD[(String, Double)] = combine.map { case (key, value) => (key, value._1 / value._2.toDouble) }
    result.collect().foreach(println)
    sc.stop()
  }
}
