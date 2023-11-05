package spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: BY
 * @date: 2019/11/1
 * @description: Spark Dataframe方式连接
 */
object SparkJdbc extends App {
  val spark = SparkSession.builder.master("local[*]").appName("MyName").getOrCreate
  val sc = spark.sparkContext
  val url = "jdbc:mysql://localhost:3306/ecif_etl_test?useSSL=false&characterEncoding=UTF-8"
  val tableName = "t_ods_bib_t_cm_clientinfo_hv"
  val prop = new java.util.Properties
  prop.setProperty("user", "root")
  prop.setProperty("password", "By9216446o6")
  prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  // 取得该表数据
  private val jdbcDF: DataFrame = spark.read.jdbc(url, tableName, prop)
  println(jdbcDF.select("Form").limit(3).collect().mkString)
  jdbcDF.select("Form").limit(3).foreach(name => println(name))
  //DF存为新的表
  //  jdbcDF.write.mode("append").jdbc(url, "tableName2", prop)
  sc.stop()
}
