package unbounded.state

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import unbounded.state.models.TemperatureReading
import unbounded.state.process.TemperatureMetricsProcessFunction
import unbounded.state.sources.TemperatureReadingGenerator

import java.time.Duration

object Pipeline {
  def main(args: Array[String]): Unit = {
    val numberOfSensors = Option(System.getProperty("numberOfSensors")).map(_.toInt).getOrElse(1000)
    val numberOfEvents = Option(System.getProperty("numberOfEvents")).map(_.toLong).getOrElse(Long.MaxValue)
    val eventsPerSecond = Option(System.getProperty("eventsPerSecond")).map(_.toInt).getOrElse(500)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Checkpoint every minute
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setCheckpointInterval(Duration.ofMinutes(1).toMillis)
    checkpointConfig.setMinPauseBetweenCheckpoints(Duration.ofMinutes(1).toMillis)
    checkpointConfig.setTolerableCheckpointFailureNumber(Int.MaxValue)

    // Create the source from the DataGenerator and assign the watermark equal to TemperatureReading's eventTimestamp
    val temperatureReadings = env.fromSource(
      TemperatureReadingGenerator.create(numberOfSensors, numberOfEvents, eventsPerSecond),
      WatermarkStrategy
        .forBoundedOutOfOrderness[TemperatureReading](Duration.ofMinutes(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[TemperatureReading] {
          def extractTimestamp(t: TemperatureReading, l: Long): Long = t.eventTimestamp
        }),
      "temperature-readings"
    )

    // Key the stream by TemperatureReading's sensorId
    val keyedTemperatureReadings = temperatureReadings
      .keyBy((t: TemperatureReading) => t.sensorId)

    // Process the keyed stream and produce TemperatureMetrics
    val temperatureMetrics = keyedTemperatureReadings
      .process(new TemperatureMetricsProcessFunction)
      .name("temperature-metrics")

    // Sink the TemperatureMetrics to STDOUT so the operator chain will be evaluated
    temperatureMetrics.print("temperature-metrics")

    env.execute("Temperature Metrics")
  }
}
