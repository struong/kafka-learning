name := "kafka-test"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.1"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.32"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.0"
libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"

Global / onChangedBuildSource := ReloadOnSourceChanges