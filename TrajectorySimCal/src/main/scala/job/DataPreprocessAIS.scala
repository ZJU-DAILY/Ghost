package job

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.core.fs.FileSystem.WriteMode

import java.nio.file.FileSystem

//import org.apache.flink.streaming.api.scala.createTypeInformation
import tools.LoadPoint
import trajectory.Point

object DataPreprocessAIS {
  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Data File Path
    env.readTextFile("")
      .filter(str => str.split(",")(0) != "MMSI")
      .map(str_line => {
        val arr_line = str_line.split(",")
        (arr_line(0), arr_line(1), arr_line(2), arr_line(3))
      })
      //      .filter(x => x._2 >= "2008-02-03 12:00:00" && x._2 <= "2008-02-03 13:00:00")
      .sortPartition(1, Order.ASCENDING)
      .map(x => {
        x._1 + "," + x._2 + "," + x._3 + "," + x._4
      })
      .writeAsText("")
    // Output Path
    env.execute("DataPreprocessAIS")
  }
}
