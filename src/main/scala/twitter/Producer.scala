package twitter

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties

object Producer {

  def createProducer(bootstrapServer: String): KafkaProducer[String, String] = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer(props)
  }

}
