package elasticSearch

import io.circe.generic.auto._
import io.circe.parser

final case class Tweet(id: Long, id_str: String, text: String)

object JsonParser {
  def parse(jsonString: String): Either[io.circe.Error, Tweet] = {
    parser.decode[Tweet](jsonString)
  }
}
