package spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/13
 * @description: 通过spark实现点击流日志分析-------TopN(求访问url地址最多的前N位)
 */
object TopN {

  def main(args: Array[String]): Unit = {
    // 1、构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    // 2、构建SparkContext对象
    val sc = new SparkContext(sparkConf)
    // 3、读取数据文件
    val data: RDD[String] = sc.textFile("./data/access.log")
    // 4、过滤出丢失的数据记录，直接舍弃掉
    val rightRDD: RDD[String] = data.filter(x => x.split("").length > 10)
    // 5、获取每条数据的url地址 (url,1)
    val urlAndOne: RDD[(String, Int)] = rightRDD.map(x => (x.split(" ")(10), 1))
    // 6、相同的url出现的1累加
    val result: RDD[(String, Int)] = urlAndOne.reduceByKey(_ + _)
    // 7、求出访问url次数最多的前5位 第二个参数默认是true表示升序，可以改为false表示降序
    val sortedRDD: RDD[(String, Int)] = result.sortBy(x => x._2, false)
    val top5: Array[(String, Int)] = sortedRDD.take(5)
    top5.foreach(println)
    sc.stop()
  }
}
