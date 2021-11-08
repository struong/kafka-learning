name := "kafka-learning"

version := "0.1"

scalaVersion := "2.13.7"

val elastic4sVersion = "7.15.1"

libraryDependencies += "org.apache.kafka"       % "kafka-clients"            % "2.8.1"
libraryDependencies += "org.slf4j"              % "slf4j-simple"             % "1.7.32"
libraryDependencies += "com.github.pureconfig"  %% "pureconfig"              % "0.17.0"
libraryDependencies += "com.twitter"            % "hbc-core"                 % "2.2.0"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-testkit"       % elastic4sVersion % "test"

Global / onChangedBuildSource := ReloadOnSourceChanges
