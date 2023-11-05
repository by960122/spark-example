package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/16
 * @description: 需求:创建一个pairRDD,取出每个分区相同key对应值的最大值,然后相加
 *               AggregateByKey: 分区内:取最大,分区间:求和 分区内 != 分区间,底层 : CombineByKey
 */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("b", 3), ("c", 4), ("c", 6), ("c", 8)), 2)
    rdd.glom().collect().foreach(array => {
      //          println(array.mkString(","))
      println(array.foreach(print))
    })
    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    aggregateByKeyRDD.collect().foreach(println)
    sc.stop()
  }
}
