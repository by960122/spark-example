package spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: BYDylan
 * @date: 2020/4/17
 * @description: spark 操作json数据
 */
object SparkJson {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("MyName").getOrCreate
    val sc = sparkSession.sparkContext
    val dataFrame: DataFrame = sparkSession.read.json(projectPath + "\\doc\\people.json")
    //    这种方式也可以,更通用
    //    val dataFrame: DataFrame = sparkSession.read.format("json").load(projectPath + "\\doc\\people.json")
    dataFrame.show()
    dataFrame.select(dataFrame("age") + 1).show()
    dataFrame.createTempView("student")
    sparkSession.sql("select * from student").show()

  }
}
