package tools

import scala.collection.mutable

case class Parameters(
                     cell_size: (Double, Double),
                     span_size: (Double, Double),
                     partition_size: (Int, Int),
                     max_trajectory_num: Int,
                     max_len_of_trajectory: Int,
                     window_size: Int,
                     sim_measure: String,
                     search_threshold_upper_bound: Double,
                     search_interval: Long,
                     LCSS_gap: Int,
                     partition_num: (Int, Int),
                     trajectory_sum: Long,
                     grid2partition: mutable.HashMap[(Long, Long), Long]
                     )
