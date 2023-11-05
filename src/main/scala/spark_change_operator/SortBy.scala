package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description:
 * sortBy:按参数排序
 * sortByKey:返回一个按照key进行排序的(K,V)的RDD
 */
object SortBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    println("sortBy: ")
    val rdd: RDD[Int] = sc.parallelize(List(2, 1, 3, 4))
    //    升序:true,降序false
    val sortByRDD: RDD[Int] = rdd.sortBy(x => x, false)
    sortByRDD.collect().foreach(println)

    println("sortByKeyRdd: ")
    val sortByKeyRdd: RDD[(Int, String)] = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    //    升序:true,降序false
    sortByKeyRdd.sortByKey(true).collect().foreach(println)
    sortByKeyRdd.sortByKey(false).collect().foreach(println)
    sc.stop()
  }
}
