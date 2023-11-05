package spark_change_operator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/16
 * @description: 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
 *               类似于 inner join
 */
object Join {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))
    rdd.join(rdd1).collect().foreach(println)
    sc.stop()
  }
}
