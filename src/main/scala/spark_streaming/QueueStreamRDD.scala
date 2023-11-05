package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author: BYDylan
 * @date: 2020/4/19
 * @description: 循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
 *               通过使用ssc.queueStream(queueOfRDDs)来创建DStream,每一个推送到这个队列中的RDD,都会作为一个DStream处理
 */
object QueueStreamRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD队列流").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val RDDQueue = new mutable.Queue[RDD[Int]]
    val queueStream = ssc.queueStream(RDDQueue)
    ssc.checkpoint("C:\\WorkSpace\\spark_example\\spark-warehouse")
    //    val result=queueStream.map(x=>(x%2,1)).reduceByKey(_+_)
    val result = queueStream.map(x => (x % 2, 1)).updateStateByKey((input: Seq[Int], output: Option[Int]) => {
      Some(input.sum + output.getOrElse(0))
    })
    result.print(1000)
    ssc.start()
    for (i <- 1 to 5) {
      RDDQueue += ssc.sparkContext.makeRDD(1 to 10, 2)
      Thread.sleep(2000) //每2秒发一次数据
    }
    ssc.stop()
  }
}
