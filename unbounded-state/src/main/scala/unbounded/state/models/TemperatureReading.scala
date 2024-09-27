package unbounded.state.models

case class TemperatureReading(
  sensorId: String,
  temperature: Double,
  eventTimestamp: Long
)
