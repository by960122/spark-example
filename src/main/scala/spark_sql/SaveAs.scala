package spark_sql

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/17
 * @description: 保存文件
 */
object SaveAs {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val foldRdd = sc.makeRDD(1 to 10, 2)
    foldRdd.saveAsTextFile(projectPath + "\\doc\\saveAsTextFile\\")
    foldRdd.saveAsObjectFile(projectPath + "\\doc\\saveAsObjectFile\\")
    sc.stop()
  }
}
