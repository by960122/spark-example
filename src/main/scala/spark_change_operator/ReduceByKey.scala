package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: groupByKey也是对每个key进行操作,但只生成一个sequence
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    //    创建rdd，使每个元素跟所在的分区形成一个元组
    val words: Array[String] = Array("one", "two", "three", "one", "two", "three")
    val listRdd: RDD[(String, Int)] = sc.makeRDD(words).map(word => (word, 1))
    val group: RDD[(String, Iterable[Int])] = listRdd.groupByKey()
    group.collect().foreach(println)
    val reduceByKeyRDD: RDD[(String, Int)] = listRdd.reduceByKey(_ + _)
    reduceByKeyRDD.collect().foreach(println)
    sc.stop()
  }
}
