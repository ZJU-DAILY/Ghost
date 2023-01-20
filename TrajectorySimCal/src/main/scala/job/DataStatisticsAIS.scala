package job

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.core.fs.FileSystem.WriteMode

import java.nio.file.FileSystem

//import org.apache.flink.streaming.api.scala.createTypeInformation
import tools.LoadPoint
import trajectory.Point

object DataStatisticsAIS {
  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.readTextFile("")
      .map(str_line => str_line.split(",")(0))
      //      .filter(x => x._2 >= "2008-02-03 12:00:00" && x._2 <= "2008-02-03 13:00:00")
      .groupBy(x => x)
      .reduce((x1, x2) => x1)
      .writeAsText("").setParallelism(1)
    env.execute("DataStatisticsAIS")
  }
}
