package tools

import java.util.ResourceBundle

/**
 * @author: BYDylan
 * @date: 2023/3/1
 * @description:
 */
object PropsTools {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def main(args: Array[String]): Unit = {
    println(PropsTools("kafka.bootstrap-servers"))
  }

  def apply(propsKey: String): String = {
    bundle.getString(propsKey)
  }
}
