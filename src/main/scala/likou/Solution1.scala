package likou

/**
 * @author: BYDylan
 * @date: 2022/10/22
 * @description: 两数相加
 *               给定一个整数数组 nums和一个整数目标值 target,请你在该数组中找出 和为目标值 target 的那两个整数,并返回它们的数组下标。
 *               你可以假设每种输入只会对应一个答案。但是,数组中同一个元素在答案里不能重复出现。
 *               输入：nums = [2,7,11,15], target = 9
 *               输出：[0,1]
 *               解释：因为 nums[0] + nums[1] == 9 ,返回 [0, 1]
 */
object Solution1 {
  def main(args: Array[String]): Unit = {
    val nums: Array[Int] = Array(5, 7, 8, 13, 16, 98)
    val result: Array[Int] = twoSum(nums, 66)
    result.foreach(println)
  }

  def twoSum(nums: Array[Int], target: Int): Array[Int] = {
    var mapIndex: Map[Int, Int] = Map()
    for (index <- nums.indices) {
      if (mapIndex.contains(target - nums(index))) return Array(mapIndex(target - nums(index)), index)
      mapIndex += (nums(index) -> index)
    }
    Array(0, 0)
  }
}