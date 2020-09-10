package com.kafka.producer

import java.io.{FileNotFoundException, FileReader}
import java.util.Properties
import java.util.concurrent.LinkedBlockingDeque

import com.kafka.producer.utils.Utilities

object TwitterProducerMain {

  def main(args: Array[String]): Unit = {
    try {
      if (args.length != 2) {
        throw new Exception(s"Incorrect number of arguments. Expected 2 (properties file, pipe-separated topics), got ${args.length}")
      }
      val kafkaProperties = new Properties()
      kafkaProperties.load(new FileReader(args(0).trim))

      val kafkaTopics = args(1).trim.split(Utilities.CLI_TOPIC_SEPARATOR)
      println("=================================================================\n")
      Utilities.createKafkaTopics(Utilities.getTopicProperties(kafkaProperties), kafkaTopics)
      println("=================================================================\n")

      for (topic <- kafkaTopics) {
        val topicNew = Utilities.replaceSpaces(topic)
        val topicThread = new TwitterProducer(kafkaProperties, new LinkedBlockingDeque[String](100000), topic.trim, topicNew)
        topicThread.setName(s"$topicNew-Twitter-Producer-Thread")
        topicThread.start()
      }
    } catch {
      case _: FileNotFoundException => println(s"File not found ${args(0).trim}================")
      case e: RuntimeException => println(e.getMessage)
      case e: Exception => println(s"Something went wrong: $e\n================")
    }
  }
}
