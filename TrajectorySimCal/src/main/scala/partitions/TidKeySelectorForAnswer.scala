package partitions

import org.apache.flink.api.java.functions.KeySelector
import trajectory.QueryAnswer

class TidKeySelectorForAnswer extends KeySelector[QueryAnswer, (Long, Long)]{
  override def getKey(in: QueryAnswer): (Long, Long) = (in.query.tid, in.query.query_time)
}
