package elasticSearch

import com.sksamuel.elastic4s.http.{JavaClient, NoOpRequestConfigCallback}
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import config.BonsaiConfig
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.slf4j.LoggerFactory
import pureconfig._
import pureconfig.generic.auto._

import java.time.Duration
import scala.jdk.javaapi.CollectionConverters.asScala

object ElasticSearchClient extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val config = ConfigSource.default.loadOrThrow[BonsaiConfig]

  val hostname = config.bonsai.hostname
  val userName = config.bonsai.userName
  val password = config.bonsai.password

  val callback = new HttpClientConfigCallback {
    override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
      val creds = new BasicCredentialsProvider()
      creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password))
      httpClientBuilder.setDefaultCredentialsProvider(creds)
    }
  }

  val props = ElasticProperties(hostname)
  val client = ElasticClient(
    JavaClient(props, requestConfigCallback = NoOpRequestConfigCallback, httpClientConfigCallback = callback))

  // we must import the dsl
  import com.sksamuel.elastic4s.ElasticDsl._

  val indexName = "twitter"

  // Next we create an index in advance ready to receive documents.
  // await is a helper method to make this operation synchronous instead of async
  // You would normally avoid doing this in a real program as it will block
  // the calling thread but is useful when testing
  client.execute {
    createIndex(indexName)
  }.await

  val consumer = Consumer()

  // poll for new data
  while (true) {
    val consumerRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
    logger.info(s"Received ${consumerRecords.count()} records")

    val requests = asScala(consumerRecords.iterator).map { record =>
      // 2 strategies to generate a an ID to ensure data is idempotent
      // 1. Kafka generic ID
      val genericId = s"${record.topic()} ${record.partition()} ${record.offset()}"

      // 2. Twitter specific ID
      JsonParser.parse(record.value()).map { tweet =>
        val id = tweet.id_str

        val value = record.value()
        indexInto(indexName).source(value).id(id).refreshImmediately
      }
    }

    val bulkRequests = requests.flatMap(_.toOption).toSeq
    client.execute(
      bulk(bulkRequests)
    )

    logger.info("Committing offsets...")
    consumer.commitSync()
    logger.info("Offsets have been committed")
    Thread.sleep(1000)

    // to find the committed entries do: GET /twitter/_doc/{id}
  }

  // close the client gracefully
  client.close()
  logger.info("Application exited")
}
