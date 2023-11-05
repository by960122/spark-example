package spark_action_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: groupByKey也是对每个key进行操作,但只生成一个sequence
 */
object CheckPoint {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionWithIndex").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    //    设置一个检查点,要配置路径
    sc.setCheckpointDir(projectPath + "\\doc\\")
    val words: Array[String] = Array("one", "two", "three", "one", "two", "three")
    val listRdd: RDD[(String, Int)] = sc.makeRDD(words).map(word => (word, 1))
    listRdd.checkpoint()
    val reduceByKeyRDD: RDD[(String, Int)] = listRdd.reduceByKey(_ + _)
    reduceByKeyRDD.collect().foreach(println)
    println(reduceByKeyRDD.toDebugString)
    sc.stop()
  }
}
