package twitter

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties

object Producer {

  def createProducer(bootstrapServer: String): KafkaProducer[String, String] = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // create safe Producer
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")


    // high throughput producer (at th expense of latency and CPU usage)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, (32*1024).toString) // 32 KB batch size

    new KafkaProducer(props)
  }

}
