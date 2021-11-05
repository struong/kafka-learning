package twitter

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.jdk.CollectionConverters.SeqHasAsJava

object Topic {
  def create(topicName: String, bootstrapServer: String): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)

    val partitions = 6
    val replicationFactor = 1.toShort

    val admin = Admin.create(props)

    // Create a compacted topic
    val newTopics = List(new NewTopic(topicName, partitions, replicationFactor)).asJava

    val result = admin.createTopics(newTopics)

    // Call values() to get the result for a specific topic
    val future = result.values().get(topicName)

    // Call get() to block until the topic creation is complete or has failed
    // if creation failed the ExecutionException wraps the underlying cause.
    try {
      future.get()
    } catch {
      case e: Exception =>
        if (!e.getCause.isInstanceOf[TopicExistsException])
          throw new RuntimeException(e)
        else
          logger.info(s"Topic $topicName already exists")
    }

  }
}
