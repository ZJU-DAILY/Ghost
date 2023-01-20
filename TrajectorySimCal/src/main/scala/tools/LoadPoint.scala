package tools
import org.apache.flink.api.common.functions.MapFunction
import trajectory.Point
import java.text.SimpleDateFormat

class LoadPoint extends MapFunction[String, Point] {

  override def map(t: String): Point = {
    val line_arr = t.split(",")
//    new Point(
//      if (line_arr(2).toDouble - 281.0 <= 0.0) 0.0 else if (
//        line_arr(2).toDouble - 3935.0 >= 23854) 23854 else line_arr(2).toDouble - 281.0,
//      if (line_arr(3).toDouble - 281.0 <= 0.0) 0.0 else if (
//        line_arr(3).toDouble - 3935.0 >= 26916) 26916 else line_arr(3).toDouble - 3935.0,
//      timeConvert(line_arr(1)), line_arr(0).toLong)
    new Point(
      if (line_arr(2).toDouble - 116.0 <= 0.0) 0.0 else if (
        line_arr(2).toDouble - 116.8 >= 0.0) 0.8 else line_arr(2).toDouble - 116.0,
      if (line_arr(3).toDouble - 39.5 <= 0.0) 0.0 else if (
        line_arr(3).toDouble - 40.3 >= 0.0) 0.8 else line_arr(3).toDouble - 39.5,
      timeConvert(line_arr(1)), line_arr(0).toLong)
  }

  private def timeConvert(timestamp_string: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    sdf.parse(timestamp_string).getTime() / 1000
//    timestamp_string.toLong
  }
}
