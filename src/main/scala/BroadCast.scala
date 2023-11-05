import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/15
 * @description: 使用广播变量优化 union,使用广播变量减少数据传输
 */
object BroadCast {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("Map").set("spark.testing.memory", "1073740000")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(Int, String)] = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val list = List((1, 1), (2, 2), (3, 3))

    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    val resultRdd: RDD[(Int, (String, Any))] = rdd.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRdd.foreach(println)
    sc.stop()
  }
}
