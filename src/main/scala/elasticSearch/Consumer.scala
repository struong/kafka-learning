package elasticSearch

import com.sksamuel.elastic4s.fields.TextField
import com.sksamuel.elastic4s.http.{JavaClient, NoOpRequestConfigCallback}
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.indexes.CreateIndexResponse
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, RequestFailure, RequestSuccess, Response}
import config.{BonsaiConfig, ServiceConfig}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.slf4j.LoggerFactory
import pureconfig._
import pureconfig.generic.auto._

object Consumer extends App {
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
      .mapping(
        properties(
          TextField("tweets")
        )
      )
  }.await

  // Next we index a single document which is just the name of an Artist.
  // The RefreshPolicy.Immediate means that we want this document to flush to the disk immediately.
  // see the section on Eventual Consistency.
  val insertResp = client.execute {
    indexInto(indexName).fields("tweets" -> "bar").refresh(RefreshPolicy.Immediate)
  }.await

  // now we can search for the document we just indexed
  val resp = client.execute {
    search(indexName).query("bar")
  }.await

  logger.info("---- Search Results ----")
  resp match {
    case failure: RequestFailure => logger.info("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] => logger.info(results.result.hits.hits.toList.toString)
    case results: RequestSuccess[_] => logger.info(results.result.toString)
  }

  // Response also supports familiar combinators like map / flatMap / foreach:
  resp foreach (search => logger.info(s"There were ${search.totalHits} total hits"))
  
  // close the client gracefully
  client.close()
  logger.info("Application exited")
}
