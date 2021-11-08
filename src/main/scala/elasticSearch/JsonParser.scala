package elasticSearch

import io.circe.generic.auto._
import io.circe.parser

final case class Tweet(id: Long, id_str: String, text: String)

object JsonParser {
  def parse(jsonString: String): Tweet = {
    val decoded = parser.decode[Tweet](jsonString)

    decoded match {
      case Left(parsingError) =>
        throw new IllegalArgumentException(s"Invalid JSON object: ${parsingError.getMessage}")
      case Right(tweet) => tweet
    }
  }
}
