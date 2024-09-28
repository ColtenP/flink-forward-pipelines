package unordered.data.process

import org.apache.flink.api.common.functions.MapFunction
import unordered.data.models.{TemperatureMetric, TemperatureReading}

class TemperatureReading2MetricMapper extends MapFunction[TemperatureReading, TemperatureMetric] {
  def map(reading: TemperatureReading): TemperatureMetric =
    TemperatureMetric(
      sensorId = reading.sensorId,
      min = reading.temperature,
      max = reading.temperature,
      avg = reading.temperature,
      count = 1
    )
}
