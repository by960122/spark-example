package spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/14
 * @description: RDD的创建
 */
object RddDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Spark_RDD").set("spark.testing.memory", "2147480000")
    //创建spark上下文对象
    val sc = new SparkContext(sparkConf)
    //1.创建rdd，从内存中创建makeRDD,底层就是 parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 5, 4), 2)
    listRDD.collect().foreach(println)
    //2.从内存中创建parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4), 2)
    arrayRDD.collect().foreach(println)
    //3.从外部存储创建,默认项目路径，也可以改为hdfs路径hdfs://hadoop102:9000/xxx
    //读取文件时，传递的参数为最小分区数，但是不一定是这个分区，取决于hadoop分片规则
    val fileRDD: RDD[String] = sc.textFile(projectPath + "\\doc\\word.txt", 2)
    fileRDD.foreach(println)
    //将内存创建RDD数据保存到文件,8核，四个数据放八个分区
    //    fileRDD.saveAsTextFile("C:\\WorkSpace\\spark_example\\doc\\")
    sc.stop()
  }

}
