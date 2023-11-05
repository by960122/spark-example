import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
 * @author: BYDylan
 * @date: 2020/4/18
 * @description: 自定义累加器
 */
object WordAccumulator {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulators").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(sparkConf)
    val dataRdd: RDD[String] = sc.makeRDD(List("hadoop", "hbase", "kafka", "spark"), 2)
    val wordAccumulator = new WordAccumulator
    sc.register(wordAccumulator, "accumulator")
    dataRdd.foreach {
      case word => {
        wordAccumulator.add(word)
      }
    }
    println("sum: " + wordAccumulator.value)
    sc.stop()
  }

  class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
    val list = new util.ArrayList[String]()

    //    当前累加器是否为初始状态
    override def isZero: Boolean = list.isEmpty

    //    复制累加器对象
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new WordAccumulator()

    //    重置累加器对象
    override def reset(): Unit = list.clear()

    //    向累加器中增加数据
    override def add(v: String): Unit = {
      if (v.contains("h")) {
        list.add(v)
      }
    }

    //    累加器合并
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = list.addAll(other.value)

    //    获取累加器的结果
    override def value: util.ArrayList[String] = list
  }

}
