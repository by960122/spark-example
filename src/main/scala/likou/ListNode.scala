package likou

/**
 * @author: BYDylan
 * @date: 2022/10/22
 * @description: 节点数对象
 */
class ListNode(_x: Int = 0, _next: ListNode = null) {
  var next: ListNode = _next
  var x: Int = _x

  override def toString: String = x.toString
}
