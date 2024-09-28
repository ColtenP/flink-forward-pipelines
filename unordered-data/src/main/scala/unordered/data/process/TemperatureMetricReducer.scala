package unordered.data.process

import org.apache.flink.api.common.functions.ReduceFunction
import unordered.data.models.TemperatureMetric

class TemperatureMetricReducer extends ReduceFunction[TemperatureMetric] {
  def reduce(agg: TemperatureMetric, curr: TemperatureMetric): TemperatureMetric =
    agg.copy(
      min = Math.min(agg.min, curr.min),
      max = Math.max(agg.max, curr.max),
      avg = ((agg.avg * agg.count) + (curr.avg * curr.count)) / (agg.count + curr.count),
      count = agg.count + curr.count
    )
}
