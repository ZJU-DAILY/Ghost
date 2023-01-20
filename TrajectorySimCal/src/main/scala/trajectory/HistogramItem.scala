package trajectory

class HistogramItem(var frequency: Int,
                    var grid_id: (Long, Long)) extends Ordered[HistogramItem]{

  var search_threshold_level: Double = 0.0
  var copy_frequency: Int = 0


  def FrequencyDecrease(): Unit = {
    this.frequency = this.frequency - 1
  }

  override def compare(that: HistogramItem): Int = {
    that.frequency.compareTo(this.frequency)
  }
}
