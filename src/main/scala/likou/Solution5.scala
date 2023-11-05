package likou

/**
 * @author: BYDylan
 * @date: 2022/10/25
 * @description: 最长回文子串
 *               给你一个字符串 s,找到 s 中最长的回文子串。
 *               示例 1：
 *               输入：s = "babad"
 *               输出："bab"
 *               解释："aba" 同样是符合题意的答案
 */
object Solution5 {
  // 中心扩散法O(N2)
  def longestPalindrome1(s: String): String = {
    var len = s.length
    if (len < 2) {
      return s
    }
    var maxLen = 1
    var res = s.substring(0, 1)
    for (i <- 0 to len - 2) {
      val oddStr = centerSpread(s, i, i)
      val evenStr = centerSpread(s, i, i + 1)
      var maxLenStr = if (oddStr.length > evenStr.length) oddStr else evenStr
      if (maxLenStr.length > maxLen) {
        maxLen = maxLenStr.length
        res = maxLenStr
      }
    }
    return res
  }

  def centerSpread(s: String, left: Int, right: Int): String = {
    // left = right 奇数
    // right = left + 1 偶数
    var len = s.length
    var i = left
    var j = right
    while (i >= 0 && j < len && s.charAt(i) == s.charAt(j)) {
      i = i - 1
      j = j + 1
    }
    return s.substring(i + 1, j)
  }

  // 动态规划
  def longestPalindrome2(s: String): String = {
    var len = s.length
    if (len < 2) {
      return s
    }
    var maxLen = 1
    var begin = 0
    // dp(i)(j) => s[i,j]是否回文串
    val dp: Array[Array[Boolean]] = Array.ofDim[Boolean](s.length, s.length)
    // for (i <- 0 to s.length - 1) {
    //   // 对角线的表示单个字符,肯定是回文串,置为true
    //   dp(i)(i) = true
    // }
    // 在对角线上方操作,即i<j
    for (j <- 1 to s.length - 1) {
      for (i <- 0 to j - 1) {
        if (s.charAt(i) != s.charAt(j)) {
          // 不想等则一定不是回文串
          dp(i)(j) = false
        } else {
          // 相等
          if (j - i <= 3) {
            // 距离小于三,表示中间最多只可能有一个字符,则在相等情况下,一定是回文串
            dp(i)(j) = true
          } else {
            // 中间有>=2个字符,需要移动i,j到里面判断下一个是否是回文,
            // dp(i+1)(j-1)再上一轮循环已经赋值了
            dp(i)(j) = dp(i + 1)(j - 1)
          }
        }
        // 如果是回文串,且长度大于临时值,则替换
        if (dp(i)(j) && j - i + 1 > maxLen) {
          maxLen = j - i + 1
          begin = i
        }
      }
    }
    return s.substring(begin, begin + maxLen)
  }
}
