package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author: BYDylan
 * @date: 2020/4/18
 * @description: 自定义聚合函数
 */
object SparkSqlUDAF {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val udaf = new MyAvgFunt
    spark.udf.register("avgage", udaf)
    val dataFrame = spark.read.json(projectPath + "\\doc\\people.json")
    dataFrame.createOrReplaceTempView("user")
    spark.sql("select avgage(age) from user").show()
  }
}

class MyAvgFunt extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //  计算时数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //  返回数据类型
  override def dataType: DataType = DoubleType

  //  函数是否稳定
  override def deterministic: Boolean = true

  //  初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //  新数据进来后的操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //  合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //  计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
