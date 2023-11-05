package spark_streaming

import org.apache.spark._
import org.apache.spark.streaming._

import scala.collection.mutable

object QueueStreamDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MyApp")
  val sc = new SparkContext(sparkConf)
  val input1 = List((1, true), (2, false), (3, false), (4, true), (5, false))
  val input2 = List((1, false), (2, false), (3, true), (4, true), (5, true))
  val rdd1 = sc.parallelize(input1)
  val rdd2 = sc.parallelize(input2)
  val ssc = new StreamingContext(sc, Seconds(3))
  val ds1 = ssc.queueStream[(Int, Boolean)](mutable.Queue(rdd1))
  val ds2 = ssc.queueStream[(Int, Boolean)](mutable.Queue(rdd2))

  //  val ds = ds1.join(ds2)
  //等价于
  val ds = ds1.transform(rdd => {
    rdd.join(rdd2)
  })
  ds.print()
  ssc.start()
  ssc.awaitTerminationOrTimeout(5000)
  ssc.stop()
}
