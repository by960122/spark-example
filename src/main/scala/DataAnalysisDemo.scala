import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Call(age: Double, job: String, marital: String, education: String,
                credit_default: String, housing: String, loan: String,
                contact: String, month: String, day: String,
                duration: Double, campaign: Double, pdays: Double,
                previous: Double, pout: String, emp_var_rate: Double,
                cons_price_idx: Double, cons_conf_idx: Double, euribor3m: Double,
                nr_employed: Double, y: String)

/**
 * @author: BYDylan
 * @date: 2020/8/2
 * @description: 探索葡萄牙银行电话调查的结果
 */
object DataAnalysisDemo {
  private val projectPath: String = System.getProperty("user.dir")

  def main(args: Array[String]) {
    // 葡萄牙银行通过电话访问进行市场调查得到数据集,以下为21个字段
    // 受访者年龄
    val age: StructField = StructField("age", DataTypes.IntegerType)
    // 受访者职业
    val job: StructField = StructField("job", DataTypes.StringType)
    // 婚姻状态
    val marital: StructField = StructField("marital", DataTypes.StringType)
    // 受教育程度
    val education: StructField = StructField("education", DataTypes.StringType)
    // 是否信贷违约
    val credit_default: StructField = StructField("credit_default", DataTypes.StringType)
    // 是否有房屋贷款
    val housing: StructField = StructField("housing", DataTypes.StringType)
    // 是否有个人贷款
    val loan: StructField = StructField("loan", DataTypes.StringType)
    // 联系类型(移动电话或座机)
    val contact: StructField = StructField("contact", DataTypes.StringType)
    // 当天访谈的月份
    val month: StructField = StructField("month", DataTypes.StringType)
    // 当天访谈时间的是星期几
    val day: StructField = StructField("day", DataTypes.StringType)
    // 最后一次电话联系持续时间
    val duration: StructField = StructField("duration", DataTypes.DoubleType)
    // 此次访谈的电话联系的次数
    val campaign: StructField = StructField("campaign", DataTypes.DoubleType)
    // 距离早前访谈最后一次电话联系的天数
    val pdays: StructField = StructField("pdays", DataTypes.DoubleType)
    // 早前访谈电话联系的次数
    val previous: StructField = StructField("previous", DataTypes.DoubleType)
    // 早前访谈的结果,成功或失败
    val pout: StructField = StructField("pout", DataTypes.StringType)
    // 就业变化率（季度指标）
    val emp_var_rate: StructField = StructField("emp_var_rate", DataTypes.DoubleType)
    // 消费者物价指数（月度指标）
    val cons_price_idx: StructField = StructField("cons_price_idx", DataTypes.DoubleType)
    // 消费者信心指数（月度指标）
    val cons_conf_idx: StructField = StructField("cons_conf_idx", DataTypes.DoubleType)
    // 欧元银行间3月拆借率
    val euribor3m: StructField = StructField("euribor3m", DataTypes.DoubleType)
    // 员工数量（季度指标）
    val nr_employed: StructField = StructField("nr_employed", DataTypes.DoubleType)
    // 目标变量,是否会定期存款
    val y: StructField = StructField("y", DataTypes.StringType)
    val fields: Array[StructField] = Array(age, job, marital,
      education, credit_default, housing, loan, contact, month, day, duration, campaign, pdays, previous, pout,
      emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed, y)
    val schema: StructType = StructType(fields)
    val spark: SparkSession = SparkSession.builder().appName("data exploration")
      .config("spark.ui.showConsoleProgress", true)
      .config("spark.testing.memory", "4294960000").master("local[2]").getOrCreate()
    // 该数据集中的记录有些字段没用采集到数据为unknown
    val dataFrame: DataFrame = spark.read.schema(schema).option("sep", ";").option("header", true)
      .csv(projectPath + "\\doc\\bank-additional-full.csv")
    println("共有 %s 行".format(dataFrame.count()))
    println("根据婚姻情况统计各类人群的数量和缺失值的数量: ")
    dataFrame.groupBy("marital").count().show(20)
    println("根据职业统计各类人群的数量和缺失值的数量: ")
    dataFrame.groupBy("job").count().show(20)
    println("根据教育情况统计各类人群的数量和缺失值的数量: ")
    dataFrame.groupBy("education").count().show(20)
    println("选数值类字段作为数据子集,进行描述性统计(包括频次统计,平均值,标准差,最小值,最大值): ")
    var dsSubset: DataFrame = dataFrame.select("age", "duration", "campaign", "previous", "y", "marital", "education").cache()
    println("通过描述性统计,可以对数据进行快速地检查: ")
    // 比如,频次统计可以检查数据的有效行数,年龄的平均值和范围可以判断数据样本是不是符合预期.通过均值和方差可以对数据进行更深入地分析
    // 比如,假设数据服从正态分布,年龄的均值和标准差表明了受访者的年龄大多在30~50 之间.
    dsSubset.describe().show(20)
    // 判断变量间相关性,计算变量间的协方差和相关系数,协方差表示两变量的变化方向相同或相反.age和duration的协方差为-2.3391469421265874,表示随着受访者的年龄增加,上一次访问时长减少.
    println("判断变量间相关性,计算变量间的协方差和相关系数: ")
    println(dsSubset.stat.cov("age", "duration"))
    // 相关系数(Pearson系数)表示变量间的相关程度.age和duration的相关系数为-8.657050101409117E-4,呈较弱的负相关性.
    println("相关系数(Pearson系数)表示变量间的相关程度: ")
    println(dsSubset.stat.corr("age", "duration"))
    // 交叉表,通过交叉表可以知道在每个年龄段的婚姻状态分布
    println("交叉表: ")
    dsSubset.stat.crosstab("age", "marital").orderBy("age_marital").show(20)
    // 所有受访人的学历背景出现频率超过0.3的学历
    println("所有受访人的学历背景出现频率超过0.3的学历 %s".format(dsSubset.stat.freqItems(Seq("education"), 0.3).collect()(0)))
    // 四分位数,第三个参数0.0表示相对误差
    println("四分位数,第三个参数0.0表示相对误差: ")
    dsSubset.stat.approxQuantile("age", Array(0.25, 0.5, 0.75), 0.0).foreach(println)
    // 聚合函数分析
    // 根据定期存款意愿将客户分组,并统计各组客户的客户总数,此次访谈的电话联系的平均次数,最后一次电话联系的平均持续时间,早前访谈电话联系的平均次数
    //     dataFrame.groupBy("y").agg(count("age").name("Total customers"),
    //         round(avg("campaign"),2).name("Avgcalls(curr)"),
    //         round(avg("duration"),2).name("Avg duration"),
    //         round(avg("previous"),2).name("AvgCalls(previous)")).withColumnRenamed("value","TDSubscribed?")
    //       .show(20)
    println("根据定期存款意愿将客户分组,并统计各组客户的客户总数,此次访谈的电话联系的平均次数,最后一次电话联系的平均持续时间,早前访谈电话联系的平均次数: ")
    dsSubset.groupBy("y").agg(Map("age" -> "count", "campaign" -> "avg", "duration" -> "avg", "previous" -> "avg")).withColumnRenamed("value", "TDSubscribed?").show(20)
    println("根据年龄将客户分组,并统计各组客户的客户总数,此次访谈的电话联系的平均次数,最后一次电话联系的平均持续时间,早前访谈电话联系的平均次数: ")
    //     dataFrame.groupBy("age")
    //       .agg(count("age").name("Total customers"),
    //         round(avg("campaign"),2).name("Avgcalls(curr)"),
    //         round(avg("duration"),2).name("Avg duration"),
    //         round(avg("previous"),2).name("AvgCalls(previous)")).orderBy("age")
    //       .show(20)
    dsSubset.groupBy("age").agg(Map("age" -> "count", "campaign" -> "avg", "duration" -> "avg", "previous" -> "avg")).orderBy("age").show(20)
  }
}
