package spark_sql

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/18
 * @description: Spark 交互 HBase
 */
object SparkHadoopHBase {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    val sc = new SparkContext(sparkConf)
    //构建HBase配置信息
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.1.201")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, "student")
    //从HBase读取数据形成RDD
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("lines: " + hbaseRDD.count())
    //对hbaseRDD进行处理
    hbaseRDD.foreach {
      case (_, result) =>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        println("RowKey:" + key + ",Name:" + name)
    }
    //关闭连接
    sc.stop()
  }
}
