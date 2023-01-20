package nodes
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import trajectory.Query
import scala.collection.mutable

class TimerQueryProcessFunction(var interval: Long) extends ProcessFunction[Query, Query]{
  @transient private var is_first_query: Boolean = _
  @transient private var search_interval: Long = _
  @transient private var query_list: mutable.Queue[Query] = _
  override def open(parameters: Configuration): Unit = {
    is_first_query = true
    search_interval = interval
    query_list = mutable.Queue[Query]()
  }

  override def processElement(i: Query, context: ProcessFunction[
    Query, Query]#Context, collector: Collector[Query]): Unit = {
    if (is_first_query) {
      is_first_query = false
      context.timerService().registerProcessingTimeTimer(
        context.timerService().currentProcessingTime() + search_interval
      )
    }
    query_list.enqueue(i)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[
    Query, Query]#OnTimerContext, out: Collector[Query]): Unit = {
    val q = query_list.dequeue()
    val cur_time = ctx.timerService().currentProcessingTime()
    ctx.timerService().registerProcessingTimeTimer(cur_time + search_interval)
    out.collect(q)
  }

}
