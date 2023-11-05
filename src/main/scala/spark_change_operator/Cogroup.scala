package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/16
 * @description: 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
 *               类似于 left join
 */
object Cogroup {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    rdd.cogroup(rdd1).collect().foreach(println)
    sc.stop()
  }
}
