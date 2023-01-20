package tools

import partitions.TidKeySelector


class PartitionLocator(var partition_size: (Int, Int), var partition_num: (Int, Int)) {

  def getPartitionIdFromGrid(grid_id: (Long, Long)): Long = {
//    println(grid_id)
//    println("experiment3")
    val initial_partition_id: (Int, Int) =
      ((grid_id._1 / partition_size._1).toInt, (grid_id._2 / partition_size._2).toInt)

////    println(initial_partition_id)
//
//    val partition_id_arr: Array[Int] = new Array(10)
//
//    var quad_tree_node: QuadTreeNode = TidKeySelector.subpartitions_depth(
//      initial_partition_id._1 * (TidKeySelector.initial_partition_num._2 + 1) + initial_partition_id._2)
//
////    println("*********************-----------------------****************")
////    println(initial_partition_id._1 * (TidKeySelector.initial_partition_num._2 + 1) + initial_partition_id._2)
////    println(quad_tree_node.depth)
////    println("*****************---------------------------****************")
//
//    var cur_partition_size = (partition_size._1 / 2, partition_size._2 / 2)
//    var cur_grid_id = ((grid_id._1 % partition_size._1).toInt, (grid_id._2 % partition_size._2).toInt)
//    var depth = quad_tree_node.depth
//
//
//    while (quad_tree_node.child.nonEmpty) {
//      partition_id_arr(depth) = (cur_grid_id._1 / cur_partition_size._1) * 2 +
//        (cur_grid_id._2 / cur_partition_size._2)
//      quad_tree_node = quad_tree_node.child(partition_id_arr(depth))
//      cur_grid_id = (cur_grid_id._1 % cur_partition_size._1, cur_grid_id._2 % cur_partition_size._2)
//      cur_partition_size = (cur_partition_size._1 / 2, cur_partition_size._2 / 2)
//      depth = quad_tree_node.depth
//    }
//
////    if (quad_tree_node.trajectory_num >= ParametersReceiver.getMaxTrajectoryNum()) {
////      for (i <- 0 to 3) {
////        quad_tree_node.child(i) = new QuadTreeNode(depth + 1)
////        for (j <- quad_tree_node.trajectory_ids(i)) {
////          if ()
////        }
////        quad_tree_node.child(i).trajectory_ids
////      }
////
////    }
//
//    depth = depth - 1
//
//    var partition_id: Long = 0
//
//    var multiply_factor = 1
//
//    while (depth > 0) {
//      partition_id = partition_id + partition_id_arr(depth) * multiply_factor
//      depth = depth - 1
//      multiply_factor = multiply_factor * 4
//    }
//
//    partition_id = (partition_id + ((initial_partition_id._1 * (TidKeySelector.initial_partition_num._2 + 1))
//    + initial_partition_id._2).toLong * 10000)
//
//    quad_tree_node.trajectory_num = quad_tree_node.trajectory_num + 1
//
////    println(partition_id)
//
////    partition_id
    ((initial_partition_id._1 * (partition_num._2 + 1))
      + initial_partition_id._2).toLong



  }
}
