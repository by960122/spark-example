package spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/14
 * @description: 单词统计
 */
object WordCount {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]) = {
    //     val config = SparkSession.builder().master("local[3]").appName("WordCount").config("spark.testing.memory","2147480000")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("WordCount").set("spark.testing.memory", "2147480000")
    // 创建spark上下文对象
    val sc = new SparkContext(sparkConf)
    // 将文件内容读取
    val lines: RDD[String] = sc.textFile(projectPath + "\\doc\\word.txt")
    // 将一行一行的数据转换为单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 将单词结构进行转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    // 对转换后的结果进行分组聚合
    val reduceByKey: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    // 将统计结果打印到控制台
    val result: Array[(String, Int)] = reduceByKey.sortBy(_._2, false).take(6)

    result.foreach(println)
    sc.stop()
  }

}
