package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: 去重算子,涉及shuffle过程
 */
object Distinct {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val makeRDD: RDD[Int] = sc.makeRDD(Array(1, 3, 2, 5, 6, 3, 2, 1), 2)
    val makedist: RDD[Int] = makeRDD.distinct()
    makedist.collect().foreach(println)
    //    makedist.saveAsTextFile("output")
    sc.stop()
  }
}
