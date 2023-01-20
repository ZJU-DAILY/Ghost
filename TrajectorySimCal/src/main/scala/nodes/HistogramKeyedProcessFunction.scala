package nodes
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import tools.{GridLocator, Parameters, PartitionLocator}
import trajectory.{HistogramItem, Point, Query}

import scala.util.control.Breaks
import java.lang.Math
import scala.collection.mutable
import scala.collection.immutable
import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}

class HistogramKeyedProcessFunction(var params: Parameters) extends KeyedCoProcessFunction[Long, Point, Query, Point]{

  @transient private var span_size: (Double, Double) = _
  @transient private var partition_num: (Int, Int) = _
  @transient private var max_len_of_trajectory: Int = _
  @transient private var window_size: Int = _
  @transient private var cell_size: (Double, Double) = _
  @transient private var sim_measure: String = _
  @transient private var partition_size: (Int, Int) = _
  @transient private var search_threshold_upperbound: Double = _
//  @transient private var search_interval: Long = _
  @transient private var trajectory_sum: Long = _
  @transient private var query_with_partitions_tag: OutputTag[Query] = _
//  @transient private var is_first_query: Boolean = _
  @transient private var histograms: mutable.HashMap[
    Long, (Int, mutable.HashMap[(Long, Long), HistogramItem], mutable.Queue[Point])] = _
  @transient private var sorted_histograms: mutable.HashMap[Long, mutable.ArrayBuffer[(Double, mutable.Set[(Long, Long)])]] = _

//  @transient private var partition_histograms: mutable.HashMap[
//    Long, mutable.HashMap[Long, HistogramItem]] = _
//
//  @transient private var partitions_pointer: mutable.HashMap[Long, mutable.Set[Long]] = _

  @transient private var candidate_grids_pointer: mutable.HashMap[Long, mutable.Set[(Long, Long)]] = _

  @transient private var query_list: mutable.Queue[Query] = _

  @transient private var grid_locator: tools.GridLocator = _

  @transient private var cell_num: (Int, Int) = _

  @transient private var grid2partition: mutable.HashMap[(Long, Long), Long] = _

  override def open(parameters: Configuration): Unit = {

    span_size = params.span_size
    partition_num = params.partition_num
    max_len_of_trajectory = params.max_len_of_trajectory
    window_size = params.window_size
    cell_size = params.cell_size
    sim_measure = params.sim_measure
    partition_size = params.partition_size
    search_threshold_upperbound = params.search_threshold_upper_bound
//    search_interval = params.search_interval
    trajectory_sum = params.trajectory_sum
    query_with_partitions_tag = OutputTag[Query]("query_tag")
//    is_first_query = true
    histograms = new mutable.HashMap[
      Long, (Int, mutable.HashMap[(Long, Long), HistogramItem], mutable.Queue[Point])]()

    sorted_histograms = new mutable.HashMap[Long, mutable.ArrayBuffer[(Double, mutable.Set[(Long, Long)])]]()

//    partition_histograms = new mutable.HashMap[
//      Long, mutable.HashMap[Long, HistogramItem]]()

    candidate_grids_pointer = new mutable.HashMap[Long, mutable.Set[(Long, Long)]]()
//    partitions_pointer = new mutable.HashMap[Long, mutable.Set[Long]]()

    for (i <- 0 to trajectory_sum.toInt) {
      candidate_grids_pointer(i) = mutable.Set[(Long, Long)]()
    }

    query_list = mutable.Queue[Query]()

    grid_locator = new GridLocator(cell_size)

    cell_num = ((span_size._1 / cell_size._1).toInt + 1, (span_size._2 / cell_size._2).toInt + 1)

    grid2partition = params.grid2partition

//    for (i <- 0 until cell_num._1) {
//      for (j <- 0 until cell_num._2) {
//        grid2partition((i, j)) = ((i / partition_size._1, j / partition_size._2),
//          ((i - (i % partition_size._1)).toLong, (j - (j % partition_size._2)).toLong), partition_size)
//      }
//    }
  }

  override def processElement1(in1: Point, context: KeyedCoProcessFunction[
    Long, Point, Query, Point]#Context, collector: Collector[Point]): Unit = {
//    println("000000000000000000000000000000000000000")
//    if (histograms == null) println("histograms is null********************************")
//    println(in1)
    in1.setGridId(grid_locator.getGridId(in1))
    updateHistograms(in1)
    val cur_candidate_grids: mutable.Set[(Long, Long)] =
      locateCandidateGridsFromHistogram(histograms(in1.tid)._1, in1)
    //      print(in1.tid)
    //      print(": ")
    //      println(cur_candidate_grids)
    val old_candidate_grids: mutable.Set[(Long, Long)] =
    candidate_grids_pointer(in1.tid) -- cur_candidate_grids
    val new_candidate_grids: mutable.Set[(Long, Long)] =
      cur_candidate_grids -- candidate_grids_pointer(in1.tid)
    val preserve_candidate_grids: mutable.Set[(Long, Long)] =
      cur_candidate_grids & candidate_grids_pointer(in1.tid)

    candidate_grids_pointer(in1.tid) = cur_candidate_grids



    for (grid <- preserve_candidate_grids) {
      val output_point: Point = in1.clone()
      output_point.setDelOrUpdate(false)
      output_point.setGridId(grid)
      output_point.setPartitionId(grid2partition(grid))
      collector.collect(output_point)
    }
    for (grid <- new_candidate_grids) {

      //      println("********************histograms(in1.tid)._3*************************")
      //      println(histograms(in1.tid)._3)
      //      println("********************histograms(in1.tid)._3*************************")
      val output_point: Point = in1.clone()
      output_point.setHistoryPoints(histograms(in1.tid)._3)
      output_point.setGridId(grid)
      output_point.setDelOrUpdate(false)
      output_point.setPartitionId(grid2partition(grid))
      collector.collect(output_point)
    }
    for (grid <- old_candidate_grids) {
      val output_point: Point = in1.clone()
      output_point.setDelOrUpdate(true)
      output_point.setGridId(grid)
      output_point.setPartitionId(grid2partition(grid))
      collector.collect(output_point)
    }
  }
//    if (in1.getPartitionInitData != null) {
//      println("******************************************8")
//      for (i <- in1.getPartitionId._2._1 to
//        in1.getPartitionId._2._1 + in1.getPartitionId._3._1 - 1) {
//        for (j <- in1.getPartitionId._2._2 to
//          in1.getPartitionId._2._2 + in1.getPartitionId._3._2 - 1)
//        grid2partition((i, j)) = in1.getPartitionId
//      }
//      collector.collect(in1)
//    }
//    else {
//
//
//
//    //    println("==================================")
////    println(in1.tid)
////    println("histogram: ")
////    println(histograms(in1.tid)._2.toList)
////    println("partition Histogram: ")
////    println(partition_histograms(in1.tid).toList)
////    println("===================================")
////    val cur_partitions: mutable.Set[(Int, Int)] = locatePartitionsFromHistograms(histograms(in1.tid)._1, in1)
////    val old_partitions: mutable.Set[(Int, Int)] = partitions_pointer(in1.tid) -- cur_partitions
////    val new_partitions: mutable.Set[(Int, Int)] = cur_partitions -- partitions_pointer(in1.tid)
////    val preserve_partitions: mutable.Set[(Int, Int)] = cur_partitions & partitions_pointer(in1.tid)
////    partitions_pointer(in1.tid) = cur_partitions
////    for (part <- preserve_partitions) {
////      val output_point: Point = in1.clone()
////      output_point.setDelOrUpdate(false)
////      output_point.setPartitionId(part)
////      collector.collect(output_point)
////    }
////    for (part <- new_partitions) {
////      val output_point: Point = in1.clone()
//////      println("********************histograms(in1.tid)._3*************************")
//////      println(histograms(in1.tid)._3)
//////      println("********************histograms(in1.tid)._3*************************")
////      output_point.setHistoryPoints(histograms(in1.tid)._3)
////      output_point.setDelOrUpdate(false)
////      output_point.setPartitionId(part)
////      collector.collect(output_point)
////    }
////    for (part <- old_partitions) {
////      val output_point = in1.clone()
////      output_point.setDelOrUpdate(true)
////      output_point.setPartitionId(part)
////      collector.collect(output_point)
////    }
//  }

  override def processElement2(in2: Query, context: KeyedCoProcessFunction[
    Long, Point, Query, Point]#Context, collector: Collector[Point]): Unit = {
    println("***************Query Enter**********************")
    println(sorted_histograms)
      if (sorted_histograms.contains(in2.tid) && histograms.contains(in2.tid)) {
        val grids: mutable.ArrayBuffer[(Double, mutable.Set[(Long, Long)])] = sorted_histograms(in2.tid)

        val query_time: Long = System.currentTimeMillis()
        println(grids)
        println(histograms(in2.tid)._2.size)
        val loop = Breaks
        loop.breakable {
          for (grid <- grids) {
            if ((in2.search_threshold >= grid._1 && sim_measure == "LCSS") ||
              (in2.search_threshold <= grid._1 && sim_measure != "LCSS")) {
              loop.break()
            }
            for (g <- grid._2) {
              val output_query: Query = in2.clone()
              output_query.partition_id = grid2partition(g)
              output_query.grid_id = g
              //        println({"part_id = " + part.toString})
              output_query.query_time = query_time
              context.output(query_with_partitions_tag, output_query)
              println(output_query)
            }
          }
        }
      }
      else {
        println("待查轨迹不存在.")
      }
  }


  private def updateHistograms(point: Point): Unit = {

    if (histograms != null && histograms.contains(point.tid)) {

      if (histograms(point.tid)._1 >= max_len_of_trajectory) {
        var step = histograms(point.tid)._1
        while (step > window_size) {
          // grid_histogram update
          val grid_id = histograms(point.tid)._3.head.getGridId
          histograms(point.tid)._2(grid_id).frequency -= 1
          histograms(point.tid)._3.dequeue()

          // partition_histogram update
//          updatePartitionHistogram(histograms(point.tid)._3.head, false)
          step = step - 1
        }

        // grid_histogram update
        histograms(point.tid) = (histograms(point.tid)._1 - (max_len_of_trajectory - window_size),
          histograms(point.tid)._2, histograms(point.tid)._3)
      }
      // grid_histogram update
      histograms(point.tid)._3.enqueue(point)
      if (histograms(point.tid)._2.contains(point.getGridId)) {
        histograms(point.tid)._2(point.getGridId).frequency += 1
      }
      else {
        histograms(point.tid)._2(point.getGridId) = new HistogramItem(1, point.getGridId)
      }
      histograms(point.tid) = (histograms(point.tid)._1 + 1,
        histograms(point.tid)._2, histograms(point.tid)._3)

      // partition_histogram update
//      updatePartitionHistogram(point, true)
    }
    else {
//      println("2222222222222222222222222222222222222222222222")
      val q = new mutable.Queue[Point]()
      q.enqueue(point)
      histograms(point.tid) = (1,
        mutable.HashMap(point.getGridId -> new HistogramItem(1, point.getGridId)),
        q)
//      updatePartitionHistogram(point, add_or_del = true)
    }
  }

  private def locateCandidateGridsFromHistogram(total_trajectory_item_num: Int,
                                                point: Point): mutable.Set[(Long, Long)] = {
    sorted_histograms(point.tid) = new mutable.ArrayBuffer[(Double, mutable.Set[(Long, Long)])]()
    val sorted_histogram = histograms(point.tid)._2.toList.sortBy(_._2.frequency)
    val candidate_grids: mutable.Set[(Long, Long)] = mutable.Set()
    val neighbor_step: Array[(Int, Int)] = Array((-1, -1), (-1, 0),
      (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1), (0, 0))
    var k_m: Int = 0
    val loop = Breaks
    var i = 0
    loop.breakable {
      for (his_item <- sorted_histogram) {
        k_m += his_item._2.frequency
        his_item._2.search_threshold_level = searchThresholdLevel(
          Math.min(cell_size._1, cell_size._2), k_m, total_trajectory_item_num,
          sim_measure)
        sorted_histograms(point.tid).append((his_item._2.search_threshold_level,
          mutable.Set[(Long, Long)]()))
        for (step <- neighbor_step) {
          val new_grid = (his_item._1._1 + step._1, his_item._1._2 + step._2)
          if (new_grid._1 >= 0 && new_grid._1 <= cell_num._1 - 1 &&
              new_grid._2 >= 0 && new_grid._2 <= cell_num._2 - 1) {
            if (! candidate_grids.contains(new_grid)) {
              sorted_histograms(point.tid)(i)._2.add(new_grid)
              candidate_grids.add(new_grid)
            }
          }
        }
        if (his_item._2.search_threshold_level > search_threshold_upperbound) {
          loop.break()
        }
        i = i + 1
      }
    }
    candidate_grids
  }

//  private def locatePartitionsFromHistograms(total_trajectory_item_num: Int,
//                                             point: Point): mutable.Set[(Int, Int)] = {
//    val partitions: mutable.Set[(Int, Int)] = mutable.Set()
//
//    val sorted_histogram = histograms(point.tid)._2.toList.sortBy(_._2.frequency)
////    sorted_histogram.foreach(x => {println(x._1)})
//    var k_m: Int = 0
//    var loop = Breaks
//    loop.breakable {
//      for (his_item <- sorted_histogram) {
//        k_m += his_item._2.frequency
//        his_item._2.search_threshold_level = searchThresholdLevel(
//          Math.min(cell_size._1, cell_size._2), k_m, total_trajectory_item_num,
//          sim_measure)
//
//        partitions.add(grid2partition(his_item._1)._1)
//        if (his_item._2.search_threshold_level > search_threshold_upperbound) {
//          loop.break()
//        }
//      }
//    }
////    partitions_pointer(point.tid) = partitions
////    println(partitions)
//    partitions
//
//  }

  private def searchThresholdLevel(cell_size: Double, k_m: Int, trajectory_length: Int,
                                   sim_measure: String): Double = {
    sim_measure match {
      case "DTW" => (2.0 * k_m - trajectory_length.toDouble) * cell_size
      case "LCSS" => 2.0 * (trajectory_length.toDouble - k_m)
      case "EDR" => (2.0 * k_m - trajectory_length.toDouble)
      case "ERP" => (2.0 * k_m - trajectory_length.toDouble) * cell_size
      case "Frechet" || "Hausdorff" => 0.0
    }
  }

//  private def updatePartitionHistogram(point: Point, add_or_del: Boolean): Unit = {
//    val partition_locator = new PartitionLocator(partition_size, partition_num)
//    val neighbor_step: Array[(Int, Int)] = Array((-1, -1), (-1, 1), (1, -1), (1, 1))
//    val parts = mutable.Set[Long]()
//    for (nei_step <- neighbor_step) {
//      parts.add(partition_locator
//        .getPartitionIdFromGrid((point.getGridId._1 + nei_step._1, point.getGridId._2 + nei_step._2)))
//    }
//    for (part <- parts) {
//      if (add_or_del) {
//        if (partition_histograms.contains(point.tid)) {
//          if (partition_histograms(point.tid).contains(part)) {
//            partition_histograms(point.tid)(part).frequency += 1
//            if (part != parts.head) {
//              partition_histograms(point.tid)(part).copy_frequency += 1
//            }
//          }
//          else {
//            partition_histograms(point.tid)(part) = new HistogramItem(1, point.getGridId)
//            if (part != parts.head) {
//              partition_histograms(point.tid)(part).copy_frequency = 1
//            }
//          }
//        }
//        else {
//          partition_histograms(point.tid) = mutable.HashMap(
//            part -> new HistogramItem(1, point.getGridId)
//          )
//          if (part != parts.head) {
//            partition_histograms(point.tid)(part).copy_frequency = 1
//          }
//        }
//      }
//      else {
//        partition_histograms(point.tid)(part).frequency -= 1
//        if (part != parts.head) {
//          partition_histograms(point.tid)(part).copy_frequency -= 1
//        }
//      }
//    }
//  }
}


