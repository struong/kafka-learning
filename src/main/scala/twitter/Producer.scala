package twitter

import org.slf4j.LoggerFactory

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

object Producer {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    logger.info("Setup")
    // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
    var messageQueue = new LinkedBlockingQueue[String](10)

    // create a twitter client
    val client = Client(messageQueue)
    client.connect()

    // create a kafka producer

    // loop to send tweets to kafka
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      try {
        logger.info(messageQueue.poll(5, TimeUnit.SECONDS))
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
          client.stop()
      }
    }

    logger.info("End of application")
  }
}
