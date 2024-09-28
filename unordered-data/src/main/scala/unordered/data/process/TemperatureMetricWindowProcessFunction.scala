package unordered.data.process

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import unordered.data.models.TemperatureMetric

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class TemperatureMetricWindowProcessFunction
  extends ProcessWindowFunction[TemperatureMetric, TemperatureMetric, String, TimeWindow] {

  def process(
               key: String,
               context: ProcessWindowFunction[TemperatureMetric, TemperatureMetric, String, TimeWindow]#Context,
               elements: lang.Iterable[TemperatureMetric],
               collector: Collector[TemperatureMetric]
             ): Unit =
    elements
      .asScala
      .foreach { metric =>
        collector.collect(
          metric.copy(
            windowStart = context.window().getStart,
            windowEnd = context.window().getEnd
          )
        )
      }
}
