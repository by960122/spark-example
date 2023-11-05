package spark_rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: BY
 * @date: 2020/4/13
 * @description: 通过spark实现ip归属地查询
 */
object Iplocation {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("Iplocation").setMaster("local[2]")
    // 2、构建SparkContext对象
    val sc = new SparkContext(sparkConf)
    // 3、加载城市ip信息数据，获取 (ip开始数字、ip结束数字、经度、纬度
    val city_ip_rdd: RDD[(String, String, String, String)] = sc.textFile("./data/ip.txt").map(x => x.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))
    // 使用spark的广播变量把共同的数据广播到参与计算的worker节点
    val cityIpBroadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(city_ip_rdd.collect())
    // 4、读取运营商日志数据
    val userIpsRDD: RDD[String] = sc.textFile("./data/20090121000132.394251.http.format").map(x => x.split("\\|")(1))
    // 5、遍历userIpsRDD 获取每一个ip地址，然后转换Long类型数字，去广播变量值中去比较
    val result: RDD[((String, String), Int)] = userIpsRDD.mapPartitions(iter => {
      // 获取广播变量的值
      val city_ip_array: Array[(String, String, String, String)] = cityIpBroadcast.value
      // 获取每一个ip地址
      iter.map(ip => {
        //  把ip地址转换成Long类型数字
        val ipNum: Long = ip2Long(ip)
        // 需要拿到ipNum数字去广播变量值中进行匹配，获取该数字在广播变量数组中的下标
        val index: Int = binarySearch(ipNum, city_ip_array)
        // 获取对应的信息
        val result: (String, String, String, String) = city_ip_array(index)
        // 封装结果数据 进行返回 ((经度，纬度),1)
        ((result._3, result._4), 1)
      })
    })
    // 6、相同经纬度出现的1累加
    val finalResult: RDD[((String, String), Int)] = result.reduceByKey(_ + _)
    finalResult.foreach(println)
    sc.stop()
  }

  // 实现把ip地址转换成Long类型数字 192.168.200.100
  def ip2Long(ip: String): Long = {
    val ips: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L
    // 遍历
    for (i <- ips) {
      ipNum = i.toLong | ipNum << 8L //  * 2^8
    }
    return ipNum
  }

  // 利用二分查询，查询到数字在数组中的下标
  def binarySearch(ipNum: Long, city_ip_array: Array[(String, String, String, String)]): Int = {
    // 定义开始下标
    var start = 0
    // 定义结束下标
    var end = city_ip_array.length - 1
    while (start <= end) {
      val middle = (start + end) / 2
      if (ipNum >= city_ip_array(middle)._1.toLong && ipNum <= city_ip_array(middle)._2.toLong) {
        // return 可以直接退出while
        return middle
      }
      if (ipNum < city_ip_array(middle)._1.toLong) {
        end = middle - 1
      }
      if (ipNum > city_ip_array(middle)._2.toLong) {
        start = middle + 1
      }
    }
    return -1
  }
}

