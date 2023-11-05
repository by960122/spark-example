import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BYDylan
 * @date: 2020/4/18
 * @description: 使用累加器来共享变量
 */
object Accumulators {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    fileAccumulator
    //    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulators").set("spark.testing.memory", "2147480000")
    //    val sc = new SparkContext(conf)
    //    val dataRdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //    val accumulator = sc.longAccumulator
    //    //    这样写无法返回结果,sum传递到executor无法返回给driver
    //    //    var sum = 0
    //    //    dataRdd.foreach(i=>sum+i)
    //    dataRdd.foreach {
    //      case i => {
    //        accumulator.add(i)
    //      }
    //    }
    //    println("sum: " + accumulator.value)
    //    sc.stop()
  }

  def fileAccumulator: Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulators").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val testFile: RDD[String] = sc.textFile(projectPath + "\\doc\\word.txt")
    val accumulatorWord: LongAccumulator = sc.longAccumulator
    //    返回的是大单词个数,包含空行
    val tmp = testFile.flatMap(line => {
      //      空行+1
      if (line == "") {
        accumulatorWord.add(1)
      }
      line.split(" ")
    })
    println("单词数: " + tmp.count() + "\n空行数: " + accumulatorWord.value)
  }
}
