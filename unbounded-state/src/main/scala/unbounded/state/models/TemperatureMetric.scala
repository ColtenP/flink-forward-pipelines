package unbounded.state.models

case class TemperatureMetric(
  sensorId: String,
  min: Double,
  max: Double,
  avg: Double,
  count: Long,
  eventTimestamp: Long
)
