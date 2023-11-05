package likou

/**
 * @author: BYDylan
 * @date: 2022/11/1
 * @description: 字符串转换整数 (atoi)
 *               请你来实现一个myAtoi(string s)函数,使其能将字符串转换成一个 32 位有符号整数（类似 C/C++ 中的 atoi 函数）。
 *               函数myAtoi(string s) 的算法如下：
 *               读入字符串并丢弃无用的前导空格
 *               检查下一个字符（假设还未到字符末尾）为正还是负号,读取该字符（如果有）。 确定最终结果是负数还是正数。 如果两者都不存在,则假定结果为正。
 *               读入下一个字符,直到到达下一个非数字字符或到达输入的结尾。字符串的其余部分将被忽略。
 *               将前面步骤读入的这些数字转换为整数（即,"123" -> 123, "0032" -> 32）。如果没有读入数字,则整数为 0 。必要时更改符号（从步骤 2 开始）。
 *               如果整数数超过 32 位有符号整数范围 [−231, 231− 1] ,需要截断这个整数,使其保持在这个范围内。具体来说,小于 −231 的整数应该被固定为 −231 ,大于 231− 1 的整数应该被固定为 231− 1 。
 *               返回整数作为最终结果。
 *               注意：
 *               本题中的空白字符只包括空格字符 ' ' 。
 *               除前导空格或数字后的其余字符串外,请勿忽略 任何其他字符。
 *               示例1：
 *               输入：s = "42"
 *               输出：42
 *               解释：加粗的字符串为已经读入的字符,插入符号是当前读取的字符。
 *               第 1 步："42"（当前没有读入字符,因为没有前导空格）
 *               ^
 *               第 2 步："42"（当前没有读入字符,因为这里不存在 '-' 或者 '+'）
 *               ^
 *               第 3 步："42"（读入 "42"）
 *               ^
 *               解析得到整数 42 。
 *               由于 "42" 在范围 [-231, 231 - 1] 内,最终结果为 42
 */
object Solution8 {
  def myAtoi(str: String): Int = {
    var tmp = ""
    var s = str
    val length = s.length
    var i = 0


    while (i < length) {
      if (tmp == "" && s(i) == ' ') { // 当字符以空字符开始时
        i += 1
      } else if (tmp == "" && (s(i) == '+' || s(i) == '-' || (s(i) >= '0' && s(i) <= '9'))) { // 当第一个字符非空时,判断非空字符是否属于数字、正号、负号
        tmp += s(i)
        i += 1
      } else if (tmp != "" && (s(i) >= '0' && s(i) <= '9')) { // # 已经找到第一个字符,且后续连续字符是数字字符
        tmp += s(i)
        i += 1
      } else { // 如果后续连续字符非数字字符,则直接停止遍历
        i = length
      }
    }

    var res: Long = 0
    var maxValue: Long = (1 << 31) - 1
    var minValue: Long = -1 * (1 << 31)

    if (tmp == "" || tmp == "+" || tmp == "-") {
      res = 0
    } else {
      var flag = true // 标志位,判断字符串是否超出Long长度
      try {
        tmp.toLong
      }
      catch {
        case e: Exception => {
          flag = false
        }
      }
      if (flag) { // 数字字符串处于long类型范围内
        if (tmp.toLong < minValue) {
          res = minValue
        }
        else if (tmp.toLong > maxValue) {
          res = maxValue
        }
        else {
          res = tmp.toLong
        }
      } else { // 数字字符串不处于long类型范围内
        if (tmp(0) == '-') {
          res = minValue
        } else {
          res = maxValue
        }
      }
    }
    res.toInt
  }
}