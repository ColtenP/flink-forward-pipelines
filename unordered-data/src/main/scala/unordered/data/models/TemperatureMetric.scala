package unordered.data.models

case class TemperatureMetric(
  sensorId: String,
  min: Double,
  max: Double,
  avg: Double,
  count: Long,
  windowStart: Long = Long.MinValue,
  windowEnd: Long = Long.MaxValue
)
