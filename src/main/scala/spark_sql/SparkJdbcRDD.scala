package spark_sql

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

/**
 * @author: BYDylan
 * @date: 2020/4/18
 * @description: Spark自带方式连接
 */
object SparkJdbcRDD {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Map").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/ecif_etl_test"
    val userName = "root"
    val passWd = "By96o122"
    val JdbcRdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from t_ods_bib_t_cm_clientinfo_hv where client_id >= ? and client_id <=?",
      800001,
      800010,
      3,
      r => (r.getInt(1), r.getString(2))
    )

    println("JdbcRdd.count: " + JdbcRdd.count())
    JdbcRdd.foreach(println)
    sc.stop()
  }
}
