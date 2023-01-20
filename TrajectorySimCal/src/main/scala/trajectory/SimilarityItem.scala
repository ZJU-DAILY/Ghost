package trajectory

import scala.collection.mutable
class SimilarityItem (var trajectory_id: Long){
  var trajectory_data: mutable.Queue[Point] = mutable.Queue()
  var similarity_data: mutable.HashMap[Long, mutable.Queue[Double]] =
    mutable.HashMap[Long, mutable.Queue[Double]]()

  def delete(): Unit = {
    trajectory_data.clear()
    similarity_data.clear()
  }

}
