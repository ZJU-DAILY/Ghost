package job

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.core.fs.FileSystem.WriteMode

import java.nio.file.FileSystem

//import org.apache.flink.streaming.api.scala.createTypeInformation
import tools.LoadPoint
import trajectory.Point

object DataPreprocessSynthetic {
  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.readTextFile("")
      .map(str_line => {
        val arr_line = str_line.split("\t")
        (arr_line(0), arr_line(1), arr_line(4), arr_line(5).toDouble, arr_line(6).toDouble)
      })
      .filter(x => x._1 != "disappearpoint")
      .map(x => (x._2, x._3, x._4, x._5))
      //      .filter(x => x._2 >= "2008-02-03 12:00:00" && x._2 <= "2008-02-03 13:00:00")
      .sortPartition(1, Order.ASCENDING)
      .map(x => {
        x._1 + "," + x._2.toString + "," + x._3.toString + "," + x._4
      })
      .writeAsText("", WriteMode.OVERWRITE)
    //    data.min(2)
    //      .map(x => {
    //        x._1 + "," + x._2.toString + "," + x._3.toString + "," + x._4
    //      })
    //      .writeAsText("hdfs://10.214.149.180:9000/user/gsh/TrajectorySimCal/test_data/processed_data_min_x")
    //
    //    data.min(3).print()
    //
    //    data.writeAsText("hdfs://10.214.149.180:9000/user/gsh/TrajectorySimCal/test_data/processed_data")
    env.execute("DataPreprocessSyntheticDataH")
  }
}
