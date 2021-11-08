package config

final case class BrokerAddress(uri: String)
final case class TopicsConfig(name: String, twitter: String)

final case class ServiceConfig(server: BrokerAddress, topics: TopicsConfig)

final case class TwitterKeys(consumerKey: String, consumerSecret: String, token: String, secret: String)
final case class TwitterConfig(twitter: TwitterKeys)

final case class BonsaiKeys(hostname: String, userName: String, password: String)
final case class BonsaiConfig(bonsai: BonsaiKeys)

