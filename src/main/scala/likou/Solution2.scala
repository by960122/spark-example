package likou

/**
 * @author: BYDylan
 * @date: 2022/10/22
 * @description: 两数相加
 *               给你两个非空 的链表，表示两个非负的整数。它们每位数字都是按照逆序的方式存储的，并且每个节点只能存储一位数字。
 *               请你将两个数相加，并以相同形式返回一个表示和的链表。
 *               你可以假设除了数字 0 之外，这两个数都不会以 0开头
 *               输入：l1 = [2,4,3], l2 = [5,6,4]
 *               输出：[7,0,8]
 *               解释：342 + 465 = 807.
 */
object Solution2 {
  // 递归写法
  def addTwoNumbers(l1: ListNode, l2: ListNode): ListNode = (l1, l2) match {
    case (l1, null) => l1
    case (null, l2) => l2
    case (_, _) =>
      val sum: Int = l1.x + l2.x
      val sumNode = new ListNode(sum % 10)
      val next = if (sum / 10 > 0)
        addTwoNumbers(new ListNode(1), addTwoNumbers(l1.next, l2.next))
      else
        addTwoNumbers(l1.next, l2.next)
      sumNode.next = next
      sumNode
  }

  def main(args: Array[String]): Unit = {
    val node1 = new ListNode(2, new ListNode(4, new ListNode(3)))
    val node2 = new ListNode(5, new ListNode(6, new ListNode(4)))
    var resultNode: ListNode = addTwoNumbers(node1, node2)
    while (resultNode != null) {
      print(resultNode + ",")
      resultNode = resultNode.next
    }
  }
}