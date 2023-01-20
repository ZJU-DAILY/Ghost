package nodes

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import trajectory.Query

class MultiQueryProcessFunction extends ProcessFunction[Query, Query]{
  override def processElement(i: Query, context: ProcessFunction[Query, Query]#Context, collector: Collector[Query]): Unit = {
    for (ii <- 1 to 10000) {
      collector.collect(new Query(ii * 8, 1.0))
    }
  }
}
