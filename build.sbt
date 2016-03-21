name := "Kafka Streams Test"

version := "1.0"

scalaVersion := "2.11.8"
scalacOptions ++= Seq("-Xexperimental")

resolvers +=
  "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.7.0",
  "org.apache.kafka" % "kafka-streams" % "0.9.1.0-cp1",
  "org.apache.kafka" % "kafka-clients" % "0.9.1.0-cp1",
  "org.apache.kafka" % "kafka_2.11"    % "0.9.0.1" % "test" classifier "test"
)