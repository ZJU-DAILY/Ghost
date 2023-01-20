package trajectory

import com.esotericsoftware.kryo.NotNull

import math.sqrt
import scala.collection.mutable

case class Point (var x: Double, var y: Double, var t: Long, var tid: Long) extends java.io.Serializable {

  private var grid_id: (Long, Long) = (0, 0)
  private var partition_id: Long = 0
  private var del_or_update: Boolean = true
  private var history_points: mutable.Queue[Point] = mutable.Queue[Point]()
  private var partition_init_data:
    (Long, mutable.HashMap[(Long, Long), mutable.HashMap[Long, SimilarityItem]]) = _

  //  def this(x: Double, y: Double, t: Long, tid: Long) {
  //    this(x, y, t, tid)
  ////    grid_id = (0, 0)
  ////    partition_id = 0
  ////    del_or_update = true
  ////    history_points = mutable.Queue[Point]()
  //  }

  def distanceTo(@NotNull o: Point): Double = {
    (this.x - o.x) * (this.x - o.x) * 0.588541 + (this.y - o.y) * (this.y - o.y)
//    (this.x - o.x) * (this.x - o.x) + (this.y - o.y) * (this.y - o.y)
  }

  override def clone(): Point = {
    val point = new Point(x, y, t, tid)
    point.grid_id = grid_id
    point
  }

  def getPartitionId: Long = partition_id

  def getGridId: (Long, Long) = grid_id

  def getDelOrUpdate: Boolean = del_or_update

  def getHistoryPoints: mutable.Queue[Point] = history_points

  def getPartitionInitData: (Long, mutable.HashMap[(Long, Long),
    mutable.HashMap[Long, SimilarityItem]]) = partition_init_data

  def setPartitionId(partition_id: Long): Unit = {
    this.partition_id = partition_id
  }

  def setGridId(grid_id: (Long, Long)): Unit = {
    this.grid_id = grid_id
  }

  def setDelOrUpdate(del_or_update: Boolean): Unit = {
    this.del_or_update = del_or_update
  }

  def setHistoryPoints(history_points: mutable.Queue[Point]): Unit = {
    this.history_points = history_points
  }

  def setPartitionInitData(data: (Long, mutable.HashMap[(Long, Long),
    mutable.HashMap[Long, SimilarityItem]])): Unit = {
    partition_init_data = data
  }
}

