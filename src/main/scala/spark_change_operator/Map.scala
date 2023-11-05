package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/14
 * @description:
 * map:对每条数据操作
 * mapValues:算子针对于(K,V)形式的类型只对V进行操作
 * mapPartitions:对一个rdd中所有的分区进行遍历(一个分区内多条数据),优于map算子,减少发到执行器的交互次数,但分区执行完才释放内存,可能内存溢出OOM
 * mapPartitionsWithIndex:携带分区号
 * flatMap:扁平化,将(key,value)转成list
 */
object Map {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[1]").setAppName("Map").set("spark.testing.memory", "1073740000")
    val sc = new SparkContext(config)

    println("map: ")
    val mapRdd: RDD[Int] = sc.makeRDD(1 to 3).map(_ * 2)
    //    val mapRdd = listRdd.map(x => x * 2)
    mapRdd.collect().foreach(println)

    println("mapValues: ")
    val mapValuesRdd: RDD[(Int, String)] = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    mapValuesRdd.mapValues(_ + "|||").collect().foreach(println)

    println("mapPartitions: ")
    val mapPartitionsRdd: RDD[Int] = sc.makeRDD(1 to 3)
    val partitions: RDD[Int] = mapPartitionsRdd.mapPartitions(datas => {
      datas.map(_ * 2) // 这一行是scala负责,并没有发送给exectutor 不算是计算
    })
    partitions.collect.foreach(println)

    println("mapPartitionsWithIndex: ")
    val mapPartitionsWithIndexRdd: RDD[Int] = sc.makeRDD(1 to 3, 2)
    val tupRDD: RDD[(Int, String)] = mapPartitionsWithIndexRdd.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号:" + num))
      }
    }
    tupRDD.collect().foreach(println)
    println("flatMap: ")
    val listRdd: RDD[Array[Int]] = sc.makeRDD(List(Array(1, 2), (Array(2, 3))))
    val mapInfo: RDD[Int] = listRdd.flatMap(datas => datas)
    mapInfo.collect().foreach(println)
    sc.stop()
  }
}
