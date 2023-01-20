package nodes

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import trajectory.QueryAnswer

class AnswerSinkFunction extends RichSinkFunction[QueryAnswer]{
  override def invoke(value: QueryAnswer, context: SinkFunction.Context): Unit = {
    super.invoke(value, context)
    println(value.query.tid.toString + ", " + value.query.search_threshold.toString
      + ", " + value.query.query_time.toString)
//    println(value.trajectory_ids)
    println(value.trajectory_ids.size.toString + ", " +
      (System.currentTimeMillis() - value.query.query_time).toString)
    println("===============================================================")
  }
}
