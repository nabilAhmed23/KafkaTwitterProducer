name := "KafkaTwitterProducer"

version := "1.0"

scalaVersion := "2.12.11"
mainClass in Compile := Some("com.kafka.producer.TwitterProducerMain")

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
  "com.twitter" % "hbc-core" % "2.2.0"
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
