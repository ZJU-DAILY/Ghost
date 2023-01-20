package job

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation, scalaNothingTypeInfo}

object QueryGenerate {
  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val queries: Array[String] = new Array[String](10000)
    for (i <- 1 to 10000) {
      queries(i - 1) = (scala.util.Random.nextInt(10357).toString + ", " +
        (scala.util.Random.nextInt(args(0).toDouble.toInt * 10000).toDouble / 10000.0).toString)
    }

    env.fromCollection(queries)
      .writeAsText("")


    env.execute("QueryGenerate")
  }
}
