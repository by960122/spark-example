package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/16
 * @description: 需求:创建一个pairRDD,计算相同key对应值的相加结果
 *               foldByKey: 分区内=分区间=相加,底层 : CombineByKey
 */
object FoldByKey {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, Int)] = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    val agg: RDD[(Int, Int)] = rdd.foldByKey(0)(_ + _)
    //        val agg = rdd.combineByKey(x => x, _ + _, _ + _) // 会丢失类型
    //        val agg = rdd.combineByKey(x => x, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y) // 查看源码得知,此处没有类型推断,所以需要声明
    agg.collect().foreach(println)
    sc.stop()
  }
}
