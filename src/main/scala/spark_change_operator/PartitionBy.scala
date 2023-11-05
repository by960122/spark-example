package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/15
 * @description: 对pairRDD进行分区操作,如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区,否则会生成ShuffleRDD,即会产生shuffle过程
 */
object PartitionBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(config)
    //    必须是 key,value对,才能有partitionBy方法
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    println("Before: " + listRDD.partitions.size)
    val partitionBy: RDD[(String, Int)] = listRDD.partitionBy(new Mypartitioner(3))
    println("After: " + partitionBy.partitions.size)
    //    partitionBy.saveAsTextFile("output")
    sc.stop()
  }
}

//自定义分区
class Mypartitioner(partitions: Int) extends Partitioner {
  //  override def numPartitions: Int = {
  //    partitions
  //  }
  //  第二种写法
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    1
  }
}