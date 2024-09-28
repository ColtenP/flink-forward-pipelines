package unordered.data

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import unordered.data.models.{TemperatureMetric, TemperatureReading}
import unordered.data.process.{TemperatureMetricReducer, TemperatureMetricWindowProcessFunction, TemperatureReading2MetricMapper}
import unordered.data.sources.TemperatureReadingGenerator

import java.time.Duration

object Pipeline {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Checkpoint every minute
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setCheckpointInterval(Duration.ofMinutes(1).toMillis)
    checkpointConfig.setMinPauseBetweenCheckpoints(Duration.ofMinutes(1).toMillis)
    checkpointConfig.setTolerableCheckpointFailureNumber(Int.MaxValue)

    // Create the source from the DataGenerator and assign the watermark equal to TemperatureReading's eventTimestamp
    val temperatureReadings = env.fromSource(
      TemperatureReadingGenerator.create(
        numberOfSensors = 100,
        numberOfEvents = Long.MaxValue,
        eventsPerSecond = 100
      ),
      WatermarkStrategy
        .forBoundedOutOfOrderness[TemperatureReading](Duration.ofMinutes(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[TemperatureReading] {
          def extractTimestamp(t: TemperatureReading, l: Long): Long = t.eventTimestamp
        }),
      "temperature-readings"
    )

    // Convert the readings into unaggregated metrics
    val unaggregatedTemperatureMetrics = temperatureReadings
      .map(new TemperatureReading2MetricMapper)
      .name("unaggregated-metrics")

    // Key the stream by TemperatureMetrics' sensorId
    val keyedUnaggregatedTemperatureMetrics = unaggregatedTemperatureMetrics
      .keyBy((t: TemperatureMetric) => t.sensorId)

    // Window the Keyed Metrics produce metrics on them every minute
    val temperatureMetrics = keyedUnaggregatedTemperatureMetrics
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(new TemperatureMetricReducer, new TemperatureMetricWindowProcessFunction)

    // Sink the TemperatureMetrics to STDOUT so the operator chain will be evaluated
    temperatureMetrics.print("temperature-metrics")

    env.execute("Temperature Metrics")
  }
}
