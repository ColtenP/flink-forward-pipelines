package unbounded.state.process

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import unbounded.state.models.{TemperatureMetric, TemperatureReading}
import unbounded.state.process.TemperatureMetricsProcessFunction.ReadingsStateDescriptor

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class TemperatureMetricsProcessFunction extends KeyedProcessFunction[String, TemperatureReading, TemperatureMetric] {
  private var readingsState: ListState[TemperatureReading] = _

  override def open(parameters: Configuration): Unit = {
    readingsState = getRuntimeContext.getListState(ReadingsStateDescriptor)
  }

  def processElement(
                      reading: TemperatureReading,
                      context: KeyedProcessFunction[String, TemperatureReading, TemperatureMetric]#Context,
                      collector: Collector[TemperatureMetric]
                    ): Unit = {
    val readingAsMetric = TemperatureMetric(
      sensorId = reading.sensorId,
      min = reading.temperature,
      max = reading.temperature,
      avg = reading.temperature,
      count = 1,
      eventTimestamp = reading.eventTimestamp
    )

    val metric = readingsState.get()
      .asScala
      .foldLeft(readingAsMetric) { (acc, curr) =>
        acc.copy(
          min = Math.min(acc.min, curr.temperature),
          max = Math.max(acc.max, curr.temperature),
          avg = ((acc.avg * acc.count) + curr.temperature) / (acc.count + 1),
          count = acc.count + 1
        )
      }

    readingsState.add(reading)
    collector.collect(metric)
  }
}

object TemperatureMetricsProcessFunction {
  val ReadingsStateDescriptor =
    new ListStateDescriptor[TemperatureReading]("TemperatureReadings", classOf[TemperatureReading])
}