import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

object ProducerDemoKeys {

  def printMetaData(metadata: RecordMetadata): String =
    s"""topic: ${metadata.topic()},
       | partition: ${metadata.partition()},
       | offset: ${metadata.offset()}
       | (ts: ${metadata.timestamp()})""".stripMargin.replace("\n", "")

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)

    // create Producer properties
    val props = new Properties()
    val bootstrapServers = "127.0.0.1:29092"
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
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

    for(i <- 1 to 10) {
      // create a producer record

      // note that round robin partitioning strategy is no longer a thing, kafka
      // now uses stick partitioning strategy https://www.confluent.io/blog/apache-kafka-producer-improvements-sticky-partitioner/
      val key = s"id_$i"
      logger.info(s"Key $key")

      val record: ProducerRecord[String, String] = new ProducerRecord("second_topic", key, s"hello world $i")
      // and send the data (synchronously, blocks the send, bad practice)
      producer.send(record, callback).get()
    }

    // flush the data
    producer.flush()
    producer.close()
  }
}
