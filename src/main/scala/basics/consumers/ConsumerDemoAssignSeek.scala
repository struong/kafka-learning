package basics.consumers

import config.ServiceConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import pureconfig._
import pureconfig.generic.auto._

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.SeqHasAsJava

object ConsumerDemoAssignSeek {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    val config = ConfigSource.default.loadOrThrow[ServiceConfig]
    val properties = new Properties

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.server.uri)
    properties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // earliest/latest/none

    // create Consumer
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)

    // assign and seek are mostly used to replay data o fetch a specific message

    // assign
    val topicPartitionToReadFrom = new TopicPartition(config.topics.name, 0)
    consumer.assign(List(topicPartitionToReadFrom).asJava)

    // seek
    val offSetToReadFrom = 15
    consumer.seek(topicPartitionToReadFrom, offSetToReadFrom)

    // poll for new data
    val numberOfMessagesToRead = 5
    var numberOfMessagesReadSoFar = 0

    while (numberOfMessagesToRead > numberOfMessagesReadSoFar) {
      val consumerRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

      consumerRecords.forEach { record =>
        logger.info(s"Key: ${record.key()}")
        logger.info(s"Value: ${record.value()}")
        logger.info(s"Partition: ${record.partition()}")
        logger.info(s"Offset: ${record.offset()}")

        numberOfMessagesReadSoFar += 1
      }
    }

    logger.info("Exiting the application")
  }
}
