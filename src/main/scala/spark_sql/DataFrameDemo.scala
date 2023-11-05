package spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

/**
 * @author: BYDylan
 * @date: 2020/4/18
 * @description: DataFrame API常用操作
 */
object DataFrameDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.master("local").appName("MyName")
      .config("spark.ui.showConsoleProgress", true).getOrCreate
    val sc: SparkContext = sparkSession.sparkContext
    //基本API
    val df: DataFrame = sparkSession.read.json(projectPath + "\\doc\\people.json")
    // 使用printSchema方法输出DataFrame的Schema信息
    df.printSchema()
    // 使用select方法来选择我们所需要的字段
    df.select("name").show()
    // 使用select方法选择我们所需要的字段，并给age字段加1
    df.select(df("name"), (df("age") + 1).alias("age")).show()
    // 使用filter方法完成条件过滤
    df.filter(df("age") > 21).show()
    // 使用groupBy方法进行分组，求分组后的总数
    df.groupBy("age").count().show()
    //sql()方法执行SQL查询操作
    //  df.registerTempTable("people")
    df.createTempView("people")
    sparkSession.sql("SELECT * FROM people").show

    //方式一：通过编程接口指定Schema
    case class Person(name: String, age: Int)
    val people: RDD[String] = sc.textFile(projectPath + "\\doc\\people.json")
    // 以字符串的方式定义DataFrame的Schema信息
    val schemaString = "name age"
    //导入所需要的类
    // 根据自定义的字符串schema信息产生DataFrame的Schema
    val schema: StructType = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //将RDD转换成Row
    val rowRDD: RDD[Row] = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    // 将Schema作用到RDD上
    val peopleDataFrame: DataFrame = sparkSession.createDataFrame(rowRDD, schema)
    println("peopleDataFrame: " + peopleDataFrame)
    // 将DataFrame注册成临时表
    peopleDataFrame.registerTempTable("people")
    val results: DataFrame = sparkSession.sql("select name from people")
    results.show

    //方式二:通过RDD创建DataFrame

    import sparkSession.implicits._

    val rdd1: RDD[(String, Int)] = sc.makeRDD(Seq(("a", 1), ("b", 1)))
    val df1: DataFrame = sparkSession.createDataset(rdd1).toDF()
    df1.withColumn("name", new Column("_1")).withColumn("age", new Column("_2")).show()

    //方式三:方式二的简化
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Seq(("a", 1), ("b", 1)))
    val df2: DataFrame = sparkSession.createDataset(rdd2).toDF("name", "age")
    df2.show()
    sc.stop()
  }
}
