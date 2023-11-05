package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/14
 * @description: 以指定的随机种子随机抽样出数量为fraction的数据
 *               withReplacement表示是抽出的数据是否放回,true为有放回的抽样,false为无放回的抽样
 *               seed用于指定随机数生成器种子
 */
object Sample {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val rdd: RDD[String] = sc.makeRDD(Array("hello1", "hello1", "hello2", "hello3", "hello4", "hello5", "hello6", "hello1", "hello1", "hello2", "hello3"))
    val sampleRDD: RDD[String] = rdd.sample(false, 0.7)
    sampleRDD.foreach(println)
    sc.stop()
  }
}
