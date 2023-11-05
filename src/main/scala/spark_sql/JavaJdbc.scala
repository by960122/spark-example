package spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author: BY
 * @date: 2019/11/1
 * @description: 标准JDBC连接方式
 */
object JavaJdbc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("MySQL")
    val sc = new SparkContext(sparkConf)
    val data: RDD[String] = sc.parallelize(List("Female", "Male", "Female"))
    //    这样遍历有个好处,conn对象在foreach内部,也就是在executor中执行,不用传输,又不用多次使用.如果放在外面无法序列化会报错
    data.foreachPartition(insertData)
    sc.stop()
  }

  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/ecif_etl_test?useSSL=false&characterEncoding=UTF-8", "root", "By9216446o6")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into kafka values (?,?,?,?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }

  def selectData(): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/ecif_etl_test?useSSL=false&characterEncoding=UTF-8"
    val c = classOf[com.mysql.cj.jdbc.Driver]
    val conn = DriverManager.getConnection(jdbcUrl, "root", "By96o122")
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    try {
      val result = statement.executeQuery("select * from t_ods_bib_t_cm_clientinfo_hv limit 3")
      while (result.next()) {
        println(result.getString(1))
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    finally {
      conn.close
    }
  }
}


