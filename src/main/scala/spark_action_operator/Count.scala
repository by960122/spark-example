package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description:
 * count:返回RDD中元素的个数
 * countByKey:返回(key,个数)
 */
object Count {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val countRdd: RDD[Int] = sc.parallelize(1 to 10)
    println("countRdd:" + countRdd.count)

    val countByKeyRdd: RDD[(Int, Int)] = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    println("countByKeyRdd:" + countByKeyRdd.countByKey)
    sc.stop()
  }
}
