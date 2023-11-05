package likou

import scala.collection.mutable._

/**
 * @author: BYDylan
 * @date: 2022/10/25
 * @description: N 字形变换
 *               将一个给定字符串 s 根据给定的行数 numRows ,以从上往下、从左到右进行Z 字形排列。
 *               比如输入字符串为 "PAYPALISHIRING"行数为 3 时,排列如下：
 *               P   A   H   N
 *               A P L S I I G
 *               Y   I   R
 *               之后,你的输出需要从左往右逐行读取,产生出一个新的字符串,比如："PAHNAPLSIIGYIR"。
 *               示例 1：
 *               输入：s = "PAYPALISHIRING", numRows = 3
 *               输出："PAHNAPLSIIGYIR"
 */
object Solution6 {
  def convert(content: String, rows: Int): String = {
    if (rows < 2) {
      return content
    }
    val resultList: Array[ArrayBuffer[Char]] = Array.fill(rows)(ArrayBuffer[Char]())
    var currentRows = 0
    var flag = -1
    for (index <- 0 until content.length) {
      resultList(currentRows).append(content.charAt(index))
      if (currentRows == 0 || currentRows == rows - 1) {
        flag =- flag
      }
      currentRows += flag
    }
    return resultList.map(_.mkString("")).mkString("")
  }
}
