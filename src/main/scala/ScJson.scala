import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods

/**
 * @author: BYDylan
 * @date: 2020/4/17
 * @description: SparkContext 操作json数据
 */
object ScJson {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(projectPath + "\\doc\\people.json")
    file.collect().foreach(x => {
      println(JsonMethods.parse(x))
    })
    sc.stop()
  }
}
