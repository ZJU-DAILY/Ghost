package tools

import trajectory.Point

class GridLocator(var cell_size: (Double, Double)) extends java.io.Serializable{


  def getGridId(point: Point): (Long, Long) = {
    ((point.x / cell_size._1).toLong, (point.y / cell_size._2).toLong)
  }



}
