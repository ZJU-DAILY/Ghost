package partitions

import org.apache.flink.api.java.functions.KeySelector
import trajectory.Query

class HistogramKeySelectorForQuery extends KeySelector[Query, Long] {
  override def getKey(in: Query): Long = in.partition_id
}
