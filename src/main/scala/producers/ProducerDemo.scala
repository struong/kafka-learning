package producers

import config.ServiceConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import pureconfig._
import pureconfig.generic.auto._

import java.util.Properties

object ProducerDemo {
  def main(args: Array[String]): Unit = {

    // create Producer properties
    val config = ConfigSource.default.loadOrThrow[ServiceConfig]
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.server.uri)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // create the producer
    val producer: KafkaProducer[String, String] = new KafkaProducer(props)

    // create a producer record
    val record: ProducerRecord[String, String] = new ProducerRecord(config.topics.name, "hello world")

    // send data
    producer.send(record)

    // flush the data
    producer.flush()
    producer.close()
  }
}
