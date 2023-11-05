package spark_sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2019/11/1
 * @description: Spark操作Hive
 */
object SparkHive extends App {
  //  System.setProperty("hadoop.home.dir", "D:\\Mysoft\\Hadoop-3.2.1")
  System.setProperty("HADOOP_USER_NAME", "root")
  private val sparkConf: SparkConf = new SparkConf().setAppName("Spark hive")
    .set("hive.metastore.urls", "thrift://192.168.1.201:9083")
    .setMaster("local[*]")
  //    .setMaster("spark://192.168.1.202:7077")
  private val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  private val sc: SparkContext = spark.sparkContext
  spark.sql("select * from hive.test_hive").show()
  spark.close()
}
