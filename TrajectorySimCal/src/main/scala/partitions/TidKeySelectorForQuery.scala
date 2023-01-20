package partitions

import org.apache.flink.api.java.functions.KeySelector
import trajectory.Query

class TidKeySelectorForQuery extends KeySelector[Query, Long]{
  override def getKey(in: Query): Long = {
    if (! TidKeySelector.partitionPointer.contains(in.tid)) 0 else
    TidKeySelector.partitionPointer(in.tid)
  }
}
