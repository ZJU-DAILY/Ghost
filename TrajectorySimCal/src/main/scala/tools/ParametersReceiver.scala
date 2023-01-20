package tools

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.net.URI


object ParametersReceiver {
  private var currentParameters: ParameterTool = null

//  currentParameters = ParameterTool.fromPropertiesFile("hdfs://10.214.149.188:9000/user/zju/TDrive/default-search.properties")
// -Dinput=hdfs://10.214.149.188:9000/user/zju/TDrive/default-search.properties

  def initFromArgs(args: Array[String]): Unit = {
    currentParameters = ParameterTool.fromPropertiesFile("")
    if (args != null && args.length != 0) {
      val argParameters = ParameterTool.fromArgs(args)
      if (currentParameters != null)
        argParameters.mergeWith(currentParameters)
      currentParameters = argParameters
    }
    else {
      println("请输入参数！")
    }
  }

  def getCellSize(): (Double, Double) = {
    (currentParameters.getDouble("cell_size_x"), currentParameters.getDouble("cell_size_y"))
  }

  def getSpanSize(): (Double, Double) = {
    (currentParameters.getDouble("span_size_x"), currentParameters.getDouble("span_size_y"))
  }

  def getPartitionSize(): (Int, Int) = {
    (currentParameters.getInt("partition_size_x"), currentParameters.getInt("partition_size_y"))
  }

  def getMaxTrajectoryNum(): Int = {
    currentParameters.getInt("max_trajectory_num")
  }

  def getMaxLenOfTrajectory(): Int = {
    currentParameters.getInt("max_len_of_trajectory")
  }

  def getWindowSize(): Int = {
    currentParameters.getInt("window_size")
  }

  def getSimMeasure(): String = {
    currentParameters.getRequired("sim_measure")
  }

  def getSearchThresholdUpperBound(): Double = {
    currentParameters.getDouble("search_threshold_upperbound")
  }

  def getSearchInterval(): Long = {
    currentParameters.getLong(("search_interval"))
  }

  def getLCSSGap(): Int = {
    currentParameters.getInt("LCSS_gap")
  }

  def getTrajectorySum(): Long = {
    currentParameters.getLong("trajectory_sum")
  }

}
