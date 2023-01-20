package tools

class QuadTreeNode(var depth: Int) {
  var child: Array[QuadTreeNode] = Array()
  var trajectory_num: Int = 0
  var trajectory_ids: Array[Array[Long]] = Array()


}
