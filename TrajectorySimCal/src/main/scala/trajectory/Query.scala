package trajectory

class Query (var tid: Long, var search_threshold: Double) extends java.io.Serializable{
  var partition_id: Long = 0
  var grid_id: (Long, Long) = (0, 0)
  var query_time: Long = 0

  override def clone(): Query = {

    new Query(tid, search_threshold)
  }
}


