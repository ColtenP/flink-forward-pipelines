package unordered.data.sources

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import unordered.data.models.TemperatureReading

import java.lang
import java.security.MessageDigest
import scala.util.Random

class TemperatureReadingGenerator(numberOfSensors: Int)
  extends GeneratorFunction[java.lang.Long, TemperatureReading] {
  @transient private lazy val random = new Random()
  @transient private lazy val sensorIds = {
    val md5 = MessageDigest.getInstance("MD5")

    Range(0, numberOfSensors)
      .map { value =>
        md5.digest(value.toString.getBytes("UTF-8"))
          .map("%02x".format(_))
          .mkString("")
          .substring(0, 8)
      }
      .toList
  }

  def map(t: lang.Long): TemperatureReading = {
    random.setSeed(t)
    val isFutureOrPast = if (random.nextDouble() > 0.8) 1L else -1L

    TemperatureReading(
      sensorId = sensorIds(t.intValue() % numberOfSensors),
      temperature = random.nextDouble() * 100,
      eventTimestamp = System.currentTimeMillis() + (3000L * (math.random() * isFutureOrPast)).toLong
    )
  }
}

object TemperatureReadingGenerator {
  def create(numberOfSensors: Int = 500, numberOfEvents: Long = Long.MaxValue, eventsPerSecond: Int = 500): DataGeneratorSource[TemperatureReading] =
    new DataGeneratorSource[TemperatureReading](
      new TemperatureReadingGenerator(numberOfSensors),
      numberOfEvents,
      RateLimiterStrategy.perSecond(eventsPerSecond),
      TypeInformation.of(classOf[TemperatureReading])
    )
}
