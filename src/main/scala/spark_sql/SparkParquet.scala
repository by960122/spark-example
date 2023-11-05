package spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: Spark SQL读写parquet文件
 */
object SparkParquet {
  private val project_path: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.master("local").appName("MyName")
      .config("spark.ui.showConsoleProgress", true).getOrCreate
    val sc = sparkSession.sparkContext
    val schema: StructType = StructType(Array(StructField("name", StringType),
      StructField("favorit_color", StringType),
      StructField("favorite_numbers", ArrayType(IntegerType))))
    val rdd: RDD[(String, String, Array[Int])] = sc.parallelize(List(("Alyssa", null, Array(3, 9, 15, 20)), ("Ben", "red", null)))
    val rowRDD: RDD[Row] = rdd.map(p => Row(p._1, p._2, p._3))
    val df: DataFrame = sparkSession.createDataFrame(rowRDD, schema)
    df.write.mode("overwrite").parquet(project_path + "\\doc\\parquent\\")
    //  通用保存方式
    //  df.write.format("json").mode("overwrite").save("C:\WorkSpace\\spark_example\doc")
    val df1: DataFrame = sparkSession.read.parquet(project_path + "\\doc\\parquent\\")
    df1.show
    df1.printSchema
  }
}
