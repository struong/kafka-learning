package consumers

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties

object ConsumerDemoGroups {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    val properties = new Properties
    val bootstrapServers = "127.0.0.1:29092"
    val groupId = "consumer-example-group-two"
    val topic = "second_topic"

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // earliest/latest/none

    // create Consumer
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)

    // subscribe consumer to our topic(s)
    consumer.subscribe(List(topic).asJava)

    // poll for new data
    while (true) {
      val consumerRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

      consumerRecords.forEach { record =>
        logger.info(s"Key: ${record.key()}")
        logger.info(s"Value: ${record.value()}")
        logger.info(s"Partition: ${record.partition()}")
        logger.info(s"Offset: ${record.offset()}")
      }
    }
  }
}
