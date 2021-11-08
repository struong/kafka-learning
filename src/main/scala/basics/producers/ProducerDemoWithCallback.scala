package basics.producers

import config.ServiceConfig
import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}
import pureconfig._
import pureconfig.generic.auto._

import java.util.Properties

object ProducerDemoWithCallback {

  def printMetaData(metadata: RecordMetadata): String =
    s"""topic: ${metadata.topic()},
       | partition: ${metadata.partition()},
       | offset: ${metadata.offset()}
       | (ts: ${metadata.timestamp()})""".stripMargin.replace("\n", "")

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)

    // create Producer properties
    val config = ConfigSource.default.loadOrThrow[ServiceConfig]

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // create the producer
    val producer: KafkaProducer[String, String] = new KafkaProducer(props)

    // create a callback
    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        // executes every time a record is successfully sent or an exception is thrown
        Option(exception)
          .map(ex => logger error s"fail to produce record due to: ${ex.getMessage}")
          .getOrElse(logger info s"successfully produced - ${printMetaData(metadata)}")
      }
    }

    for (i <- 1 to 10) {
      // create a producer record
      val record: ProducerRecord[String, String] = new ProducerRecord(config.topics.name, s"hello world $i")
      // and send the data
      producer.send(record, callback)
    }

    // flush the data
    producer.flush()
    producer.close()
  }
}
