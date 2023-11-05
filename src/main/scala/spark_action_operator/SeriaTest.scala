package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/17
 * @description: 网络传输必须要序列化
 */
object SeriaTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    val search = new Search("h")
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)
    sc.stop()
  }
}

// 网络中传递的数据必须要进行序列化
class Search(string: String) extends Serializable {

  def getMatche1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  def isMatch(s: String): Boolean = {
    s.contains(string)
  }

  def getMatche2(rdd: RDD[String]): RDD[String] = {
    val q: RDD[String] = rdd //  作为一个变量在driver中执行,不需要序列化
    q.filter(x => x.contains(string))
  }
}
