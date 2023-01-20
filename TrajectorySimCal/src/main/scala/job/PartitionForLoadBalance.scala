package job

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import tools.{GridLocator, Parameters, ParametersReceiver}

import scala.collection.mutable.ListBuffer

//import org.apache.flink.streaming.api.scala.createTypeInformation
import tools.LoadPoint
import trajectory.Point
import scala.collection.mutable

object PartitionForLoadBalance {
  def main(args: Array[String]) {
    // set up the batch execution environment
    ParametersReceiver.initFromArgs(args)
    val grid_num = ((23854 / tools.ParametersReceiver.getCellSize()._1).toInt,
      (26916 / tools.ParametersReceiver.getCellSize()._2).toInt)
    val a = tools.ParametersReceiver.getWindowSize()
    val b = tools.ParametersReceiver.getMaxLenOfTrajectory() - a
    val param1 = ((2 * a * a + b) * 1.0 / 4).toLong
    val param2 = (a * a * b + 0.5 * a * b * b).toLong
    val param3 = (a * a * a * 1.0 / b + a * a / 2.0).toLong
    val env = ExecutionEnvironment.getExecutionEnvironment
    val grid_locator: GridLocator = new GridLocator(tools.ParametersReceiver.getCellSize())
//    sampling(env, grid_locator, 0, 0)
//    var k = 0
//    for (i <- 2 to 4) {
//      val start_time = 1201910400 + (i - 2) * 86400
//      sampling(env, grid_locator, k, start_time)
//      sampling(env, grid_locator, k + 1, start_time + 3600 * 4)
//      sampling(env, grid_locator, k + 2, start_time + 3600 * 9)
//      k = k + 3
//    }

    env.readTextFile("")
      .map(x => {
        val str_arr = x.split(",")
//
//        grid_map(str_arr)
        ((str_arr(0).toLong, str_arr(1).toLong),
          (param1 * str_arr(2).toLong * str_arr(3).toLong +
            param2 * str_arr(2).toLong - param3 * str_arr(3).toLong))
      })
      .groupBy(x => x._1)
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .map(x => {
      val grid_map: mutable.HashMap[(Long, Long), Long] =
        mutable.HashMap[(Long, Long), Long]()
      for (i <- 0 to grid_num._1) {
        for (j <- 0 to grid_num._2) {
          grid_map((i.toLong, j.toLong)) = 0
        }
      }
      grid_map(x._1) = x._2
      grid_map
    })
      .groupBy(x => 1)
      .reduce((x1, x2) => {
        for (item <- x2) {
          x1(item._1) += item._2
        }
        x1
      })
      .map(x => {
        val grid2partition = partition(x, 200)
        var ans_str = ""
        for (i <- 0 to grid_num._1) {
          for (j <- 0 to grid_num._2) {
            ans_str += i.toString + "," + j.toString +
              "," + grid2partition((i, j)).toString + "\n"
          }
        }
        ans_str
      })
      .writeAsText("").setParallelism(1)



    env.execute("PartitionForLoadBalance")
  }

  def sampling(env: ExecutionEnvironment, grid_locator: GridLocator, id: Int, start_time: Long): Unit = {
    env.readTextFile("")
      .map(new LoadPoint())
      .filter(point => point.t >= start_time && point.t <= start_time + 5000)
      .map(point => {
        point.setGridId(grid_locator.getGridId(point))
        val tra_set: mutable.Set[Long] = mutable.Set()
        tra_set.add(point.tid)
        (point.getGridId, 1, tra_set)})

      .groupBy(x => x._1)
      .reduce((x1, x2) => {(x1._1, x1._2 + x2._2, x1._3 ++ x2._3)})

      .map(x => x._1._1.toString + "," + x._1._2.toString + "," + x._2.toString + "," + x._3.size.toString)

      .writeAsText("" + id.toString).setParallelism(1)
  }

  def partition(grid_info: mutable.HashMap[(Long, Long), Long], partition_num: Long):
    mutable.HashMap[(Long, Long), Long] = {
    val grid2partition: mutable.HashMap[(Long, Long), Long] =
      mutable.HashMap[(Long, Long), Long]()
    var part_id = 0
    var sum: Long = 0
    var max: Long = 0
    for (grid <- grid_info) {
      max = Math.max(max, grid._2)
    }
    val scale_size = (max * 1.0 / (partition_num * 100)).toLong
    for (grid <- grid_info) {
      grid_info(grid._1) /= scale_size
      sum += grid_info(grid._1)
    }
    var avg = sum / partition_num
    val sorted_grid_info = grid_info.toList.sortBy(_._2)(Ordering.Long.reverse)
    var cur_grid = sorted_grid_info.head
    var cur_grid_idx = 0
    while (cur_grid._2 > avg) {
      grid2partition(cur_grid._1) = part_id
      part_id += 1
      grid_info -= cur_grid._1
      cur_grid_idx += 1
      cur_grid = sorted_grid_info(cur_grid_idx)
      sum -= cur_grid._2
      avg = (sum) / (partition_num - part_id)
    }
    while (grid_info.nonEmpty) {
      sum -= bagPacking(grid_info, avg, grid2partition, part_id)
      if (partition_num > part_id + 1) {
        part_id += 1
      }
      avg = sum / (partition_num - part_id)
    }
    grid2partition
  }

  def bagPacking(grid_info: mutable.HashMap[(Long, Long), Long], bag_size: Long,
                 grid2partition: mutable.HashMap[(Long, Long), Long], part_id: Int): Long = {
    val grid_info_list = grid_info.toList
    var dp: Array[Array[Long]] = new Array[Array[Long]](grid_info_list.size + 1)
    for (i <- 0 to grid_info_list.size) {
      dp(i) = new Array[Long](bag_size.toInt + 1)
      for (j <- 0 to bag_size.toInt) {
        dp(i)(j) = 0
      }
    }

    for (i <- 1 to grid_info_list.size) {
      for (j <- 0 to bag_size.toInt) {
        dp(i)(j) = dp(i - 1)(j)
        if (j >= grid_info_list(i - 1)._2) {
          dp(i)(j) = Math.max(dp(i)(j),
            dp(i - 1)(j - grid_info_list(i - 1)._2.toInt) + grid_info_list(i - 1)._2)
        }
      }
    }
    var vol = bag_size.toInt
    var i = grid_info_list.size
    while (i >= 1) {
      if (vol >= grid_info_list(i - 1)._2 &&
        dp(i)(vol) == dp(i - 1)(vol - grid_info_list(i - 1)._2.toInt)
        + grid_info_list(i - 1)._2) {
        vol -= grid_info_list(i - 1)._2.toInt
        grid2partition(grid_info_list(i - 1)._1) = part_id
        grid_info -= grid_info_list(i - 1)._1
      }
      i -= 1
    }
    dp(grid_info_list.size)(bag_size.toInt)
  }
}
