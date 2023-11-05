package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: Coalesce,减少分区,少量合并没问题,但大量合并有可能造成数据倾斜
 */
object Coalesce {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val makeRdd: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("Before: " + makeRdd.partitions.size)
    // 参数2:是否有shuffle过程
    val coalesceRdd: RDD[Int] = makeRdd.coalesce(3, false)
    //    coalesceRdd.saveAsTextFile("output")
    println("After: " + coalesceRdd.partitions.size)
    sc.stop()
  }
}
