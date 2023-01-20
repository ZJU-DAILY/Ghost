package partitions

import org.apache.flink.api.java.functions.KeySelector
import trajectory.Point

class HistogramKeySelector extends KeySelector[Point, Long]{
  override def getKey(in: Point): Long = in.getPartitionId
}
