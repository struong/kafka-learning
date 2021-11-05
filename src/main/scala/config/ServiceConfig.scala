package config

final case class BrokerAddress(uri: String)
final case class TopicsConfig(name: String)

case class ServiceConfig(server: BrokerAddress, topics: TopicsConfig)

