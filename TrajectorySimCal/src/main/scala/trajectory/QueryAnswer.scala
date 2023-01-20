package trajectory
import scala.collection.mutable

class QueryAnswer (var query: Query){
  var trajectory_ids: mutable.Set[Long] = mutable.Set[Long]()
  var time_cost: Long = 0
}
