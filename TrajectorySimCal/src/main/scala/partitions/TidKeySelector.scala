package partitions

import org.apache.flink.api.java.functions.KeySelector
import trajectory.Point
import tools.{GridLocator, ParametersReceiver, PartitionLocator, QuadTreeNode}

import scala.collection.mutable
import scala.collection.mutable.HashMap

class TidKeySelector extends KeySelector[Point, Long] {


  override def getKey(in: Point): Long = {

//    in.tid
    val grid_id = TidKeySelector.grid_locator.getGridId(in)
    in.setGridId(grid_id)
    if (TidKeySelector.partitionPointer.contains(in.tid)) {
      TidKeySelector.partitionPointer(in.tid)
    }
    else {
      TidKeySelector.partition_locator.getPartitionIdFromGrid(grid_id)
    }
  }

}

object TidKeySelector{
//  var span_size: (Double, Double) = ParametersReceiver.getSpanSize()
//  var cell_size: (Double, Double) = ParametersReceiver.getCellSize()
//  var partition_size: (Int, Int) = ParametersReceiver.getPartitionSize()
  var span_size: (Double, Double) = (0.8, 0.8)
  var cell_size: (Double, Double) = (0.015, 0.0115)
  var partition_size: (Int, Int) = (16, 16)
  var partitionPointer = new mutable.HashMap[Long, Long]

  var trajectoryLocation = new mutable.HashMap[Long, (Array[Array[Long]], Long)]
  var subpartitions_depth: Array[QuadTreeNode] = new Array(100)
//  var partition_depth = new mutable.HashMap[Long, Int]
  var grid_locator: GridLocator = new GridLocator(cell_size)

  var initial_partition_num: (Int, Int) = ((span_size._1 / cell_size._1 / partition_size._1).toInt + 2,
    (span_size._2 / cell_size._2 / partition_size._2).toInt + 2)
  var partition_locator: PartitionLocator = new PartitionLocator(partition_size, initial_partition_num)
  for (i <- 0 to this.initial_partition_num._1) {
    for (j <- 0 to this.initial_partition_num._2) {
      subpartitions_depth(i * (this.initial_partition_num._2 + 1) + j) = new QuadTreeNode(0)
    }
  }


}
