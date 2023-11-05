package likou

import scala.collection.mutable
import scala.collection.mutable.Set

/**
 * @author: BYDylan
 * @date: 2022/10/22
 * @description: 无重复字符的最长串
 *               输入: s = "abcabcbb"
 *               输出: 3
 *               解释: 因为无重复字符的最长子串是 "abc",所以其长度为 3
 *
 */
object Solution3 {
  def main(args: Array[String]): Unit = {
    println(lengthOfLongestSubstring("abcabcbb"))
  }

  def lengthOfLongestSubstring(content: String): Int = {
    val set: mutable.Set[Char] = Set()
    val length: Int = content.length
    var rightIndex: Int = 0
    var result: Int = 0
    for (index <- content.indices) {
      set.remove(content.charAt(index))
      while (rightIndex + 1 < length && !set.contains(content.charAt(rightIndex + 1))) {
        set.add(content.charAt(rightIndex + 1))
        rightIndex += 1
      }
      result = Math.max(result, rightIndex - index)
    }
    result
  }
}
