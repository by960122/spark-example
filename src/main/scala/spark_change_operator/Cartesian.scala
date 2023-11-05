package spark_change_operator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/15
 * @description: 笛卡尔积(尽量避免使用)
 */
object Cartesian {
  def main(args: Array[String]) {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(sparkConfig)
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    val rdd3 = rdd1.intersection(rdd2)
    rdd1.cartesian(rdd2).collect().foreach(println)
    sc.stop()
  }
}
