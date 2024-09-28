package unordered.data.models

case class TemperatureReading(
  sensorId: String,
  temperature: Double,
  eventTimestamp: Long
)
