package spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/13
 * @description: 通过spark实现点击流日志分析 PV
 */
object PvUv {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    // 1、构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    // 2、构建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3、读取数据文件
    val data: RDD[String] = sc.textFile(projectPath + "\\doc\\agent.log")
    // 4、统计pv
    val pv: Long = data.count()
    println("pv: " + pv)
    // 5、获取用户的唯一标识 ip
    val ipRDD: RDD[String] = data.map(x => x.split(" ")(0))
    // 6、统计uv
    val uv: Long = ipRDD.distinct().count()
    println("uv: " + uv)
    sc.stop()
  }
}
