package twitter

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Constants, HttpHosts}
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import config.TwitterConfig
import pureconfig._
import pureconfig.generic.auto._

import java.util.concurrent.BlockingQueue
import scala.jdk.CollectionConverters.SeqHasAsJava

object Client {
  def apply(messageQueue: BlockingQueue[String]): BasicClient = {
    // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint

    // Optional: set up some followings and track terms
    // followings -> follow people
    // val followings = List(1234L, 566788L).map(long2Long).asJava
    // hosebirdEndpoint.followings(followings)
    val termToSearchFor = "bitcoin"
    val terms = List(termToSearchFor).asJava
    hosebirdEndpoint.trackTerms(terms)

    val config = ConfigSource.default.loadOrThrow[TwitterConfig]
    val consumerKey = config.twitter.consumerKey
    val consumerSecret = config.twitter.consumerSecret
    val token = config.twitter.token
    val secret = config.twitter.secret

    val hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret)

    val builder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(messageQueue))

    builder.build()
  }
}
