import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties

object ProducerDemo {
  def main(args: Array[String]): Unit = {

    // create Producer properties
    val props = new Properties()
    val bootstrapServers = "127.0.0.1:29092"
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // create the producer
    val producer: KafkaProducer[String, String] = new KafkaProducer(props)

    // create a producer record
    val record: ProducerRecord[String, String] = new ProducerRecord("first_topic", "hello world")

    // send data
    producer.send(record)

    // flush the data
    producer.flush()
    producer.close()
  }
}
