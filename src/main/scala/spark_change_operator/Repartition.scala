package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: 相比于Coalesce,它更智能,解决了数据倾斜的问题,查看源码发现它就调用的coalesce(,true)
 */
object Repartition {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val parallelize: RDD[Int] = sc.parallelize(1 to 16, 4)
    println("Before: " + parallelize.partitions.size)
    val rerdd: RDD[Int] = parallelize.repartition(2)
    println("After: " + rerdd.partitions.size)
    //    可以把分区放在一个数组里,比较方便
    val glom: RDD[Array[Int]] = rerdd.glom()
    glom.collect().foreach(array => {
      println(array.mkString(","))
    })
    sc.stop()
  }
}
