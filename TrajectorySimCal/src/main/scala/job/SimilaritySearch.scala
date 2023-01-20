package job

import nodes.{AnswerSinkFunction, HistogramKeyedProcessFunction, MultiQueryProcessFunction, SimilarityKeyedProcessFunction, TimerQueryProcessFunction}
import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, IterativeStream, KeyedStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.ProcessFunction

import scala.io.Source
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import partitions.{HistogramKeySelector, HistogramKeySelectorForQuery, TidKeySelector, TidKeySelectorForAnswer, TidKeySelectorForQuery}
import trajectory.{Point, Query, QueryAnswer}
import tools.{LoadPoint, LoadQuery, Parameters, ParametersReceiver}

import scala.collection.mutable



object SimilaritySearch {

  private val query_with_partitions_tag = OutputTag[Query]("query_tag")
  private val iteration_point_tag = OutputTag[Point]("iteration_point_tag")

  def main(args: Array[String]) {

    ParametersReceiver.initFromArgs(args)
    val grid2partition_file = Source.fromFile("")
    val grid2partition: mutable.HashMap[(Long, Long), Long] = mutable.HashMap[(Long, Long), Long]()
    for (line <- grid2partition_file.getLines()) {
      if (line != "") {
        val str_arr = line.split(",")
        grid2partition((str_arr(0).toLong, str_arr(1).toLong)) = str_arr(2).toLong
      }
    }

    val parameters: Parameters = Parameters(
      ParametersReceiver.getCellSize(),
      ParametersReceiver.getSpanSize(),
      ParametersReceiver.getPartitionSize(),
      ParametersReceiver.getMaxTrajectoryNum(),
      ParametersReceiver.getMaxLenOfTrajectory(),
      ParametersReceiver.getWindowSize(),
      ParametersReceiver.getSimMeasure(),
      ParametersReceiver.getSearchThresholdUpperBound(),
      ParametersReceiver.getSearchInterval(),
      ParametersReceiver.getLCSSGap(),
      ((ParametersReceiver.getSpanSize()._1 / ParametersReceiver.getCellSize()._1
        / ParametersReceiver.getPartitionSize()._1).toInt + 1,
        (ParametersReceiver.getSpanSize()._2 / ParametersReceiver.getCellSize()._2
        / ParametersReceiver.getPartitionSize()._2).toInt + 1),
      ParametersReceiver.getTrajectorySum(),
      grid2partition
    )

    println(parameters)

    // set up the batch execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    apply(env, parameters)

    env.execute("Similarity Search")
  }

  def apply(env: StreamExecutionEnvironment, parameters: Parameters): Unit = {

    val point_stream: SingleOutputStreamOperator[Point] = {
      env.readTextFile("")
      .map(new LoadPoint())
//      .filter(point => point.tid % 5 == 0 || point.tid % 5 == 1 || point.tid % 5 == 2 )
    }
    //        .keyBy((in: Point) => in.tid)
//      .keyBy(new TidKeySelector())
//      .process(new HistogramKeyedProcessFunction())
//      .keyBy(new HistogramKeySelector())
//      .process()


    val query_stream: KeyedStream[Query, Long] = {
      env.socketTextStream("", 6543, "\n")
      .map(new LoadQuery())
//        .process(new MultiQueryProcessFunction())
//      .filter((t: Query) => TidKeySelector.partitionPointer.contains(t.tid))
        .keyBy((in: Query) => in.tid)

    }
    //      .keyBy(new TidKeySelectorForQuery())

//    val iteration_point: IterativeStream[Point] = point_stream.iterate()

    val point_with_partitions: SingleOutputStreamOperator[Point] =
      point_stream
      .keyBy((in: Point) => in.tid)
      .connect(query_stream)
      .process(new HistogramKeyedProcessFunction(parameters))


    val query_with_partitions: DataStream[Query] =
      point_with_partitions
        .getSideOutput(query_with_partitions_tag)



    val feed_back_iteration: SingleOutputStreamOperator[QueryAnswer] =
      point_with_partitions
      .keyBy(new HistogramKeySelector())
      .connect(query_with_partitions.keyBy(new HistogramKeySelectorForQuery()))
      .process(new SimilarityKeyedProcessFunction(parameters))

//    iteration_point.closeWith(feed_back_iteration.getSideOutput(iteration_point_tag))

    feed_back_iteration
      .keyBy(new TidKeySelectorForAnswer())
      .reduce((t: QueryAnswer, t1: QueryAnswer) => {
        t.trajectory_ids = t.trajectory_ids ++ t1.trajectory_ids
        t.time_cost = t.time_cost + t1.time_cost
        t
      })
      .addSink(new AnswerSinkFunction())
  }
}