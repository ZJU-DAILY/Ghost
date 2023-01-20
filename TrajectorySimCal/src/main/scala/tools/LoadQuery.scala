package tools

import org.apache.flink.api.common.functions.MapFunction
import partitions.TidKeySelector
import trajectory.Query

class LoadQuery extends MapFunction[String, Query]{
  override def map(t: String): Query = {
    val str_arr = t.split(",")
//    println("Load Query")
//    println(TidKeySelector.partitionPointer.nonEmpty)
//    println(TidKeySelector.partitionPointer.contains(str_arr(0).toLong))

    new Query(str_arr(0).toLong, str_arr(1).toDouble)
  }
}
