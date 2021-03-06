package twitter

import config.ServiceConfig
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import pureconfig._
import pureconfig.generic.auto._

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

object TwitterApp extends App {
    // To run the command line consumer:
    // kafka-console-consumer --bootstrap-server localhost:29092 --topic twitter_topic.v1
    val logger = LoggerFactory.getLogger(getClass)

    logger.info("Setup")
    val config = ConfigSource.default.loadOrThrow[ServiceConfig]
    val topicName = config.topics.twitter
    val serverAddress = config.server.uri

    // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
    val messageQueue = new LinkedBlockingQueue[String](10)

    // create a twitter client
    val client = Client(messageQueue, List("bitcoin, politics, lfc"))
    client.connect()

    // create the topic
    Topic.create(topicName, serverAddress)

    // create a kafka producer
    val producer = Producer.createProducer(serverAddress)
    sys.addShutdownHook {
      logger.info("Shutting down application")
      logger.info("Stopping client")
      client.stop()
      logger.info("Stopping producer")
      producer.close()
    }

    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        def printMetaData(metadata: RecordMetadata): String =
          s"""topic: ${metadata.topic()},
             | partition: ${metadata.partition()},
             | offset: ${metadata.offset()}
             | (ts: ${metadata.timestamp()})""".stripMargin.replace("\n", "")

        // executes every time a record is successfully sent or an exception is thrown
        Option(exception)
          .map(ex => logger error s"fail to produce record due to: ${ex.getMessage}")
          .getOrElse(logger info s"successfully produced - ${printMetaData(metadata)}")
      }
    }

    // loop to send tweets to kafka
    while (!client.isDone()) {
      try {
        val message = messageQueue.poll(5, TimeUnit.SECONDS)
        logger.info(message)
        val producerRecord = new ProducerRecord[String, String](topicName, message)
        producer.send(producerRecord, callback)
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
          client.stop()
      }
    }

    logger.info("End of application")
}
