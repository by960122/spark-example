package spark_change_operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/16
 * @description: 需求:统计出每一个省份广告被点击次数的TOP3
 *               TS Province City User AD
 *               1516609143867 6 7 64 16
 *               1516609143869 9 4 75 18
 *               1516609143869 1 7 87 12
 */
object Practice {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    val sc = new SparkContext(sparkConf)
    val line: RDD[String] = sc.textFile(projectPath + "\\doc\\agent.log")
    //    按照最小粒度聚合:((Province,AD),1)
    val provinceAdToOne: RDD[((String, String), Int)] = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    //    计算每个省中每个广告被点击的总数:((Province,AD),sum)
    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_ + _)
    //    将省份作为key,广告加点击数为value：(Province,(AD,sum))
    val provinceToAdSum: RDD[(String, (String, Int))] = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))
    //    将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()
    //    对同一个省份所有广告的集合进行排序并取前3条,排序规则为广告点击总数
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }
    provinceAdTop3.collect().foreach(println)
    sc.stop()
  }
}
