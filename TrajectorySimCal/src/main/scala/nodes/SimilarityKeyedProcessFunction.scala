package nodes

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import tools.Parameters
import trajectory.{Point, Query, QueryAnswer, SimilarityItem}

import scala.collection.mutable

class SimilarityKeyedProcessFunction(var params: Parameters)
  extends KeyedCoProcessFunction[Long, Point, Query, QueryAnswer]{

  @transient private var max_trajectory_num: Long = _
  @transient private var max_len_of_trajectory: Int = _
  @transient private var window_size: Int = _
  @transient private var sim_measure: String = _
  @transient private var cell_size: (Double, Double) = _
  @transient private var LCSS_gap: Int = _

  @transient private var re_partition_data_tag: OutputTag[Point] = _

  @transient private var trajectories_similarity:
    mutable.HashMap[(Long, Long), mutable.HashMap[Long, SimilarityItem]] = _

  @transient private var trajectory_num: Long = _

  override def open(parameters: Configuration): Unit = {
    max_trajectory_num = params.max_trajectory_num
    max_len_of_trajectory = params.max_len_of_trajectory
    window_size = params.window_size
    sim_measure = params.sim_measure
    cell_size = params.cell_size
    LCSS_gap = params.LCSS_gap

    re_partition_data_tag = OutputTag[Point]("re_partition_data_tag")

    trajectories_similarity = new mutable.HashMap[(Long, Long),
      mutable.HashMap[Long, SimilarityItem]]()

    trajectory_num = 0
  }

  override def processElement1(in1: Point, context: KeyedCoProcessFunction[
    Long, Point, Query, QueryAnswer]#Context, collector: Collector[QueryAnswer]): Unit = {
//    println("process4------------------------------------")

    if (in1.getDelOrUpdate) {
      //        println("Delete Trajectory" + in1.tid)
      //        println(in1.getPartitionInitData)
      if (trajectories_similarity.contains(in1.getGridId) &&
        trajectories_similarity(in1.getGridId).contains(in1.tid)) {
        trajectories_similarity(in1.getGridId)(in1.tid).delete()
        for (trajectory <- trajectories_similarity(in1.getGridId)) {
          if (trajectory._1 == in1.tid && trajectory._2.similarity_data.contains(in1.tid)) {
            trajectory._2.similarity_data -= in1.tid
          }
        }
        trajectory_num = trajectory_num - 1
      }
    }
    else {
      if (! trajectories_similarity.contains(in1.getGridId)) {
        trajectories_similarity(in1.getGridId) = new mutable.HashMap[Long, SimilarityItem]()
      }
      //        print(trajectories_similarity(in1.getGridId).size)
      //        print(" ,")

      //        println("********************************")
      //        println(in1.getGridId)
      //        println("***********************************")
      if (trajectories_similarity(in1.getGridId).contains(in1.tid)) {
        trajectories_similarity(in1.getGridId)(in1.tid).trajectory_data.enqueue(in1)
        if (trajectories_similarity(in1.getGridId)(in1.tid).trajectory_data.size >= max_len_of_trajectory) {
          var step = max_len_of_trajectory
          while (step > window_size) {
            trajectories_similarity(in1.getGridId)(in1.tid).trajectory_data.dequeue()
            step -= 1
          }
          trajectories_similarity(in1.getGridId)(in1.tid).similarity_data.clear()
          initSimilarityData(in1)
        }
        else {
//          var in_time = System.currentTimeMillis()
//          var test = 0
//          for (i <- 0 to 1000000) {
//            test += i
//          }
//          println((System.currentTimeMillis() - in_time).toString + "t ")
//          in_time = System.currentTimeMillis()
          updateSimilarityData(in1)
//         print((System.currentTimeMillis() - in_time).toString + " ")
        }
      }
      else {
        trajectories_similarity(in1.getGridId)(in1.tid) = new SimilarityItem(in1.tid)
        trajectories_similarity(in1.getGridId)(in1.tid).trajectory_data = in1.getHistoryPoints
        //        println("-------------------------------")
        //        println(trajectories_similarity(in1.tid).trajectory_data)
        //        println("experiment4---------------------------")
        initSimilarityData(in1)
        trajectory_num = trajectory_num + 1
        //          println(trajectory_num)
        //          if (trajectory_num > max_trajectory_num) {
        //            rePartition(context, in1.getPartitionId)
        //          }
      }
      //        println(trajectories_similarity(in1.getGridId)(in1.tid).trajectory_data.size)
    }
//    System.gc()

//    if (in1.getPartitionInitData != null) {
//      println("*******************Repartition*******************************")
//      trajectories_similarity = in1.getPartitionInitData._2
//      trajectory_num = in1.getPartitionInitData._1
//      if (trajectory_num > max_trajectory_num) {
//        rePartition(context, in1.getPartitionId)
//      }
//    }
//    else {
////      println("Point In=====================================")
////      var time_cost = System.currentTimeMillis()
//
//
////      println(in1.toString + ", Time Cost: " + (System.currentTimeMillis() - time_cost).toString)
////      println("Point Out=======================================")
//    }
  }

  override def processElement2(in2: Query, context: KeyedCoProcessFunction[
    Long, Point, Query, QueryAnswer]#Context, collector: Collector[QueryAnswer]): Unit = {
    println("*************process query************")
    val query_ans = new QueryAnswer(in2)
    var time_start = System.currentTimeMillis()
    if (trajectories_similarity.contains(in2.grid_id) &&
      trajectories_similarity(in2.grid_id).contains(in2.tid)) {
      if (sim_measure == "LCSS") {
        for (trajectory <- trajectories_similarity(in2.grid_id)(in2.tid).similarity_data) {
          if (trajectory._2.last >= in2.search_threshold) {
            query_ans.trajectory_ids.add(trajectory._1)
            collector.collect(query_ans)
          }
        }
      }
      else {
        for (trajectory <- trajectories_similarity(in2.grid_id)(in2.tid).similarity_data) {
          if (trajectory._2.last <= in2.search_threshold &&
            trajectories_similarity(in2.grid_id)(trajectory._1).similarity_data.size >= window_size) {
            query_ans.trajectory_ids.add(trajectory._1)
          }
        }
      }
    }
//    query_ans.time_cost += (System.currentTimeMillis() - time_start)
    collector.collect(query_ans)
  }


//  private def rePartition(context: KeyedCoProcessFunction[Long, Point, Query, QueryAnswer]
//    #Context, partition_id: ((Int, Int), (Long, Long), (Int, Int))): Unit = {
////    println("repartition before: " + trajectory_num)
//    if (partition_id._3._1 >= 2 && partition_id._3._2 >= 2) {
//      val trajectories_similarity1 = new mutable.HashMap[(Long, Long),
//        mutable.HashMap[Long, SimilarityItem]]()
//      val trajectories_similarity2 = new mutable.HashMap[(Long, Long),
//        mutable.HashMap[Long, SimilarityItem]]()
//      val trajectories_similarity3 = new mutable.HashMap[(Long, Long),
//        mutable.HashMap[Long, SimilarityItem]]()
//      var trajectory_num1 = 0
//      var trajectory_num2 = 0
//      var trajectory_num3 = 0
//      for (grid <- trajectories_similarity) {
//        if (grid._1._1 >= partition_id._2._1 && grid._1._2 >= partition_id._2._2 &&
//          grid._1._1 <= partition_id._3._1 + partition_id._2._1 - 1 &&
//          grid._1._2 <= partition_id._3._2 + partition_id._2._2 - 1) {
////          println("grid = " + grid._1.toString() + ", partition_id = " + partition_id.toString())
//          val part = ((grid._1._1 - partition_id._2._1) / (partition_id._3._1 / 2),
//            (grid._1._2 - partition_id._2._2) / (partition_id._3._2 / 2))
////          println("area: " + part.toString())
//          part match {
//            case (0, 0) => {}
//            case (0, 1) => {
//              trajectories_similarity1(grid._1) = grid._2
//              trajectories_similarity -= grid._1
//              trajectory_num1 += grid._2.size
//              trajectory_num -= grid._2.size
//            }
//            case (1, 0) => {
//              trajectories_similarity2(grid._1) = grid._2
//              trajectories_similarity -= grid._1
//              trajectory_num2 += grid._2.size
//              trajectory_num -= grid._2.size
//            }
//            case (1, 1) => {
//              trajectories_similarity3(grid._1) = grid._2
//              trajectories_similarity -= grid._1
//              trajectory_num3 += grid._2.size
//              trajectory_num -= grid._2.size
//            }
//          }
//        }
//      }
//      val new_size = (partition_id._3._1 / 2, partition_id._3._2 / 2)
//      val initial_part_id = (partition_id._1._1 / 1000, partition_id._1._2 / 1000)
//      val repartition_id = (partition_id._1._1 - initial_part_id._1,
//        partition_id._1._2 - initial_part_id._2)
////      if (trajectory_num >= max_trajectory_num) {
////        rePartition(context, (partition_id._1, partition_id._2, new_size))
////      }
//      val output_point = new Point(0.0, 0.0, 0, scala.util.Random.nextInt(10357))
//
//      output_point.setPartitionInitData((trajectory_num1, trajectories_similarity1))
//      output_point.setPartitionId(((initial_part_id._1 + repartition_id._1 * 2,
//        initial_part_id._2 + repartition_id._2 * 2 + 1),
//        (partition_id._2._1, partition_id._2._2 + new_size._2),
//        (new_size._1, partition_id._3._2 - new_size._2)))
//      context.output(re_partition_data_tag, output_point)
//
//      output_point.setPartitionInitData((trajectory_num2, trajectories_similarity2))
//      output_point.setPartitionId(((initial_part_id._1 + repartition_id._1 * 2 + 1,
//        initial_part_id._2 + repartition_id._2 * 2),
//        (partition_id._2._1 + new_size._1, partition_id._2._2),
//        (partition_id._3._1 - new_size._1, new_size._2)))
//      context.output(re_partition_data_tag, output_point)
//
//      output_point.setPartitionInitData((trajectory_num3, trajectories_similarity3))
//      output_point.setPartitionId(((initial_part_id._1 + repartition_id._1 * 2 + 1,
//        initial_part_id._2 + repartition_id._2 * 2 + 1),
//        (partition_id._2._1 + new_size._1, partition_id._2._2 + new_size._2),
//        (partition_id._3._1 - new_size._1, partition_id._3._2 - new_size._2)))
//      context.output(re_partition_data_tag, output_point)
////      if (trajectory_num1 >= max_trajectory_num) {
////        rePartition(context,
////          ((initial_part_id._1 + repartition_id._1 * 2,
////            initial_part_id._2 + repartition_id._2 * 2 + 1),
////            (partition_id._2._1, partition_id._2._2 + new_size._2),
////            (new_size._1, partition_id._3._2 - new_size._2)))
////      }
////      else {
////        output_point.setPartitionInitData((trajectory_num1, trajectories_similarity1))
////        context.output(re_partition_data_tag, output_point)
////      }
////      if (trajectory_num2 >= max_trajectory_num) {
////        rePartition(context,
////          ((initial_part_id._1 + repartition_id._1 * 2 + 1,
////            initial_part_id._2 + repartition_id._2 * 2),
////            (partition_id._2._1 + new_size._1, partition_id._2._2),
////            (partition_id._3._1 - new_size._1, new_size._2)))
////      }
////      else {
////        output_point.setPartitionInitData((trajectory_num2, trajectories_similarity2))
////        context.output(re_partition_data_tag, output_point)
////      }
////      if (trajectory_num3 >= max_trajectory_num) {
////        rePartition(context,
////          ((initial_part_id._1 + repartition_id._1 * 2 + 1,
////            initial_part_id._2 + repartition_id._2 * 2 + 1),
////            (partition_id._2._1 + new_size._1, partition_id._2._2 + new_size._2),
////            (partition_id._3._1 - new_size._1, partition_id._3._2 - new_size._2)))
////      }
////      else {
////        output_point.setPartitionInitData((trajectory_num3, trajectories_similarity3))
////        context.output(re_partition_data_tag, output_point)
////      }
//    }
//    println("repartition_after: " + trajectory_num)
//  }

  private def initSimilarityData(point: Point): Unit = {
    if (trajectories_similarity(point.getGridId).contains(point.tid) &&
      trajectories_similarity(point.getGridId)(point.tid).trajectory_data.nonEmpty) {
      for (trajectory <- trajectories_similarity(point.getGridId)) {
        if (trajectory._1 != point.tid &&
          trajectory._2.trajectory_data.nonEmpty)
         {
          val sim_data_row = new mutable.Queue[Double]()
          var sim_data_col = new mutable.Queue[Double]()
          val ERP_g = Point(0.0, 0.0, 0, 0)
          var ERP_side_value1 = 0.0
          var ERP_side_value2 = 0.0
          sim_data_col.enqueue(sim_measure match {
            case "DTW" => 1.0e9
            case "LCSS" => 0.0
            case "EDR" => 1.0
            case "ERP" => {
              trajectories_similarity(point.getGridId)(point.tid)
                .trajectory_data.head.distanceTo(ERP_g)
            }
            case "Frechet" => 0.0
            case "Hausdorff" => 0.0
          })
          var tra_item_id = 1

          for (tra_item_col <- trajectory._2.trajectory_data) {
            sim_measure match {
              case "DTW" => if (sim_data_col.size == 1)
                sim_data_col.enqueue(tra_item_col.distanceTo(
                  trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head))
              else sim_data_col.enqueue(sim_data_col.last +
                tra_item_col.distanceTo(
                  trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head))

              case "LCSS" => sim_data_col.enqueue(
                if (tra_item_col.distanceTo(trajectory._2.trajectory_data.head) <=
                  0.00000992838125 &&
                  tra_item_id - 1 < LCSS_gap) 1.0 else 0.0)

              case "EDR" => sim_data_col.enqueue(
                Math.min(Math.min(sim_data_col.last + 1.0, tra_item_id + 1.0),
                  if (tra_item_col.distanceTo(
                    trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head)
                    <= 0.00000992838125) 0.0 +
                    tra_item_id - 1 else 1.0 + tra_item_id - 1.0))

              case "ERP" => {
                ERP_side_value2 += tra_item_col.distanceTo(ERP_g)
                sim_data_col.enqueue(
                  Math.min(Math.min(ERP_side_value1 +
                    tra_item_col.distanceTo(trajectories_similarity(point.getGridId)(point.tid)
                      .trajectory_data.head),
                    ERP_side_value2 + trajectories_similarity(point.getGridId)(point.tid)
                      .trajectory_data.head.distanceTo(ERP_g)),
                    sim_data_col.last + trajectory._2.trajectory_data.head.distanceTo(ERP_g)
                  )
                )
                ERP_side_value1 = ERP_side_value2
              }

              case "Frechet" => if (sim_data_col.size == 1)
                sim_data_col.enqueue(tra_item_col.distanceTo(
                  trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head))
              else sim_data_col.enqueue(Math.max(sim_data_col.last,
                tra_item_col.distanceTo(
                  trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head)))

              case "Hausdorff" => if (sim_data_col.size == 1)
                sim_data_col.enqueue(tra_item_col.distanceTo(
                  trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head))
              else sim_data_col.enqueue(Math.min(sim_data_col.last,
                tra_item_col.distanceTo(
                  trajectories_similarity(point.getGridId)(point.tid).trajectory_data.head)))

            }
            tra_item_id += 1
          }

          sim_data_row.enqueue(sim_data_col.last)

          tra_item_id = 1
//           println("========================================")
//           println("tid: " + point.tid.toString)
//           println(point.getGridId)
          for (tra_item_row <- trajectories_similarity(point.getGridId)(point.tid)
            .trajectory_data) {
//            println(tra_item_row.getGridId)
            if (tra_item_id > 1) {
              val old_col = sim_data_col
              sim_data_col = updateSimDataCol(point.getGridId, sim_data_col, trajectory._1,
                tra_item_row, tra_item_id)
              old_col.clear()
              sim_data_row.enqueue(sim_data_col.last)
            }
            tra_item_id += 1
          }

          if (! trajectories_similarity(point.getGridId)(trajectory._1)
            .similarity_data.contains(point.tid)) {
            trajectories_similarity(point.getGridId)(trajectory._1)
              .similarity_data(point.tid) = mutable.Queue[Double]()
            trajectories_similarity(point.getGridId)(trajectory._1)
              .similarity_data(point.tid).enqueue(sim_measure match {
              case "DTW" => 1.0e9
              case "LCSS" => 0.0
              case "EDR" => 1.0 * sim_data_col.size
              case "ERP" => {
                ERP_side_value2
              }
              case "Frechet" => 1.0e9
              case "Hausdorff" => 0.0
            })
          }

          while (sim_data_row.nonEmpty) {
            trajectories_similarity(point.getGridId)(trajectory._1)
              .similarity_data(point.tid).enqueue(sim_data_row.dequeue())
          }
          trajectories_similarity(point.getGridId)(point.tid)
            .similarity_data(trajectory._1) = sim_data_col
//          println("lllllllllllllllllllllllllllllllllllllllllllllll")
//          println(tid.toString + ", " + trajectory._1.toString)
//          println(trajectories_similarity(tid).similarity_data(trajectory._1))
//          println("llllllllllllllllllllllllllllllllllllllllllllllll")
        }
      }
    }

  }

  private def updateSimilarityData(point: Point): Unit = {
    if (trajectories_similarity(point.getGridId).contains(point.tid) &&
      trajectories_similarity(point.getGridId)(point.tid).trajectory_data.nonEmpty &&
    trajectories_similarity(point.getGridId)(point.tid).similarity_data.nonEmpty) {
//      print(":" + trajectories_similarity(point.getGridId)(point.tid)
//        .similarity_data.size.toString + " ")

      for (trajectory <- trajectories_similarity(point.getGridId)(point.tid)
        .similarity_data) {

//        val in_time = System.nanoTime()
        if (trajectories_similarity(point.getGridId)(trajectory._1)
          .similarity_data.contains(point.tid)) {
          val old_col = trajectories_similarity(point.getGridId)(point.tid)
            .similarity_data(trajectory._1)
//          println("ppppppppppppppppppppppppppppppppppppppppppppppppp")
//          println(tid.toString + ", " + trajectory._1.toString)
//          println(trajectories_similarity(tid).similarity_data(trajectory._1))
//          println("pppppppppppppppppppppppppppppppppppppppppppppppppppp")
          trajectories_similarity(point.getGridId)(point.tid)
            .similarity_data(trajectory._1) =
            updateSimDataCol(point.getGridId, old_col, trajectory._1,
              trajectories_similarity(point.getGridId)(point.tid).trajectory_data.last,
              trajectories_similarity(point.getGridId)(point.tid).trajectory_data.size)
          old_col.clear()
            trajectories_similarity(point.getGridId)(trajectory._1)
              .similarity_data(point.tid).enqueue(
              trajectories_similarity(point.getGridId)(point.tid)
                .similarity_data(trajectory._1).last
            )
        }
//        if (System.nanoTime() - in_time >= 100000) {
//          print((System.nanoTime() - in_time).toString + "& ")
//        }
      }
    }


  }

  private def updateSimDataCol(grid_id: (Long, Long), cur_col: mutable.Queue[Double],
                               tid: Long, new_point: Point, step: Int): mutable.Queue[Double] = {
    val new_col: mutable.Queue[Double] = mutable.Queue[Double]()
//    val new_col_1: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer[Double](1.0, 1.0
//    , 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
//      1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
//      1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val ERP_g = Point(0.0, 0.0, 0, 0)
    new_col.enqueue(sim_measure match {
      case "DTW" => 1.0e9
      case "LCSS" => 0.0
      case "EDR" => cur_col.head + 1.0
      case "ERP" => {
        new_point.distanceTo(ERP_g) + cur_col.head
      }
      case "Frechet" => {
        new_point.distanceTo(ERP_g) + cur_col.head
      }
      case "Hausdorff" => {
        new_point.distanceTo(ERP_g) + cur_col.head
      }
    })
    var col_step = 1
//    println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
//    println(cur_col)
//    println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
    var value1 = cur_col.dequeue()
    var value2 = cur_col.head
    val size = trajectories_similarity(grid_id)(tid).trajectory_data.size
//    var in_time = System.nanoTime()

    for (tra_item_col <- trajectories_similarity(grid_id)(tid).trajectory_data) {
//      val in_time = System.nanoTime()
      sim_measure match {
        case "DTW" => new_col.enqueue(Math.min(Math.min(value1, value2), new_col.last))

        case "LCSS" => new_col.enqueue(
          if (tra_item_col.distanceTo(new_point) <=
            500.0 && Math.abs(step - col_step) < LCSS_gap) 1.0
          else Math.max(value2, new_col.last))

        case "EDR" => {
          new_col.enqueue(1.0)
          new_col.enqueue(
          Math.min(Math.min(new_col.last + 1.0, value2 + 1.0),
            if (tra_item_col.distanceTo(
              trajectories_similarity(grid_id)(tid).trajectory_data.head)
              <= 500.0) 0.0 + value1 else 1.0 + value2))
        }

        case "ERP" => new_col.enqueue(
            Math.min(Math.min(value1 + tra_item_col.distanceTo(new_point),
              value2 + new_point.distanceTo(ERP_g)),
              new_col.last + tra_item_col.distanceTo(ERP_g)
            )
          )

        case "Frechet" => new_col.enqueue(Math.max(Math.min(value1, value2), new_col.last), tra_item_col.distanceTo(new_point))

        case "Hausdorff" => new_col.enqueue(Math.min(new_col.last, tra_item_col.distanceTo(new_point)))
      }
      new_col += 1.0
//      if (System.nanoTime() - in_time >= 100000) {
//        print((System.nanoTime() - in_time).toString + "& ")
//      }
      value2 = value1
      value1 = cur_col.dequeue()
      col_step += 1
//      print((System.nanoTime() - in_time).toString + " ")
    }
    col_step = 0
//    for (i <- 0 until size) {
//      val in_time = System.nanoTime()
//      new_col_1(i) = 2.0
//      if (System.nanoTime() - in_time >= 10000) {
//        print((System.nanoTime() - in_time).toString + "& ")
//      }
//      col_step += 1
//    }

//    if (System.nanoTime() - in_time >= 100000) {
//      print(col_step.toString + "+" +
//        (System.nanoTime() - in_time).toString + "& ")
//      var test = 0
//      in_time = System.nanoTime()
//      for (i <- 0 to 10000) {
//        test += i
//      }
//      if (System.nanoTime() - in_time >= 100000) {
//        print(col_step.toString + "+" +
//          (System.nanoTime() - in_time).toString + "t ")
//      }
//    }
//    print((System.nanoTime() - in_time).toString + " ")
    new_col
  }
}
