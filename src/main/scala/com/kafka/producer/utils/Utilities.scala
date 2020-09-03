package com.kafka.producer.utils

import java.util
import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.ProducerConfig

object Utilities {

  val CLI_TOPIC_SEPARATOR = "\\|"

  val TWITTER_API_KEY_PROPERTY = "twitter.api.key"
  val TWITTER_API_SECRET_PROPERTY = "twitter.api.secret"
  val TWITTER_TOKEN_PROPERTY = "twitter.token"
  val TWITTER_TOKEN_SECRET_PROPERTY = "twitter.token.secret"

  private val BOOTSTRAP_SERVERS_PROPERTY = "bootstrap.servers"
  private val GROUP_ID_PROPERTY = "group.id"
  private val ENABLE_AUTO_COMMIT_PROPERTY = "enable.auto.commit"
  private val AUTO_COMMIT_INTERVAL_MS_PROPERTY = "auto.commit.interval.ms"
  private val KEY_DESERIALIZER_PROPERTY = "key.deserializer"
  private val VALUE_DESERIALIZER_PROPERTY = "value.deserializer"

  private val KEY_SERIALIZER_PROPERTY = "key.serializer"
  private val VALUE_SERIALIZER_PROPERTY = "value.serializer"
  private val ENABLE_IDEMPOTENCE_PROPERTY = "enable.idempotence"
  private val COMPRESSION_TYPE_PROPERTY = "compression.type"
  private val LINGER_MS_PROPERTY = "linger.ms"
  private val BATCH_SIZE_PROPERTY = "batch.size"

  private val DEFAULT_ENABLE_IDEMPOTENCE = "true"
  private val DEFAULT_COMPRESSION_TYPE = "snappy"
  private val DEFAULT_LINGER_MS = "20"
  private val DEFAULT_BATCH_SIZE = s"${64 * 1024}"

  def getTopicProperties(kafkaProperties: Properties): Properties = {
    val propertyKeys = kafkaProperties.stringPropertyNames()
    if (!(propertyKeys.contains(BOOTSTRAP_SERVERS_PROPERTY) &&
      propertyKeys.contains(GROUP_ID_PROPERTY) &&
      propertyKeys.contains(ENABLE_AUTO_COMMIT_PROPERTY) &&
      propertyKeys.contains(AUTO_COMMIT_INTERVAL_MS_PROPERTY) &&
      propertyKeys.contains(KEY_DESERIALIZER_PROPERTY) &&
      propertyKeys.contains(VALUE_DESERIALIZER_PROPERTY))) {
      throw new Exception("Properties file missing one of:" +
        s"\n$BOOTSTRAP_SERVERS_PROPERTY" +
        s"\n$GROUP_ID_PROPERTY" +
        s"\n$ENABLE_AUTO_COMMIT_PROPERTY" +
        s"\n$AUTO_COMMIT_INTERVAL_MS_PROPERTY" +
        s"\n$KEY_DESERIALIZER_PROPERTY" +
        s"\n$VALUE_DESERIALIZER_PROPERTY" +
        s"\n================")
    }

    val topicProperties = new Properties()
    topicProperties.setProperty(BOOTSTRAP_SERVERS_PROPERTY, kafkaProperties.getProperty(BOOTSTRAP_SERVERS_PROPERTY))
    topicProperties.setProperty(GROUP_ID_PROPERTY, kafkaProperties.getProperty(GROUP_ID_PROPERTY))
    topicProperties.setProperty(ENABLE_AUTO_COMMIT_PROPERTY, kafkaProperties.getProperty(ENABLE_AUTO_COMMIT_PROPERTY))
    topicProperties.setProperty(AUTO_COMMIT_INTERVAL_MS_PROPERTY, kafkaProperties.getProperty(AUTO_COMMIT_INTERVAL_MS_PROPERTY))
    topicProperties.setProperty(KEY_DESERIALIZER_PROPERTY, kafkaProperties.getProperty(KEY_DESERIALIZER_PROPERTY))
    topicProperties.setProperty(VALUE_DESERIALIZER_PROPERTY, kafkaProperties.getProperty(VALUE_DESERIALIZER_PROPERTY))

    println("Topic properties:")
    topicProperties.stringPropertyNames().forEach(prop => println(s"$prop: ${topicProperties.getProperty(prop)}"))

    topicProperties
  }

  def getProducerProperties(kafkaProperties: Properties): Properties = {
    synchronized {
      val propertyKeys = kafkaProperties.stringPropertyNames()
      if (!(propertyKeys.contains(BOOTSTRAP_SERVERS_PROPERTY) &&
        propertyKeys.contains(KEY_SERIALIZER_PROPERTY) &&
        propertyKeys.contains(VALUE_SERIALIZER_PROPERTY))) {
        throw new Exception("Properties file missing one of:" +
          s"\n$BOOTSTRAP_SERVERS_PROPERTY" +
          s"\n$KEY_SERIALIZER_PROPERTY" +
          s"\n$VALUE_SERIALIZER_PROPERTY" +
          s"\n================")
      }

      val producerProperties = new Properties()
      producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty(BOOTSTRAP_SERVERS_PROPERTY))
      producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(KEY_SERIALIZER_PROPERTY))
      producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(VALUE_SERIALIZER_PROPERTY))

      if (propertyKeys.contains(ENABLE_IDEMPOTENCE_PROPERTY)) {
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaProperties.getProperty(ENABLE_IDEMPOTENCE_PROPERTY))
      } else {
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, DEFAULT_ENABLE_IDEMPOTENCE)
      }

      if (propertyKeys.contains(ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProperties.getProperty(COMPRESSION_TYPE_PROPERTY))
      } else {
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, DEFAULT_COMPRESSION_TYPE)
      }

      if (propertyKeys.contains(ProducerConfig.LINGER_MS_CONFIG)) {
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, kafkaProperties.getProperty(LINGER_MS_PROPERTY))
      } else {
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_LINGER_MS)
      }

      if (propertyKeys.contains(ProducerConfig.BATCH_SIZE_CONFIG)) {
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProperties.getProperty(BATCH_SIZE_PROPERTY))
      } else {
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, DEFAULT_BATCH_SIZE)
      }

      println("Producer properties:")
      producerProperties.stringPropertyNames().forEach(prop => println(s"Producer $prop: ${producerProperties.getProperty(prop)}"))

      producerProperties
    }
  }

  def createKafkaTopics(topicProperties: Properties, topicNames: Array[String]): Unit = {
    val adminClient = AdminClient.create(topicProperties)
    val newTopics = new util.ArrayList[NewTopic]()
    for (topicName <- topicNames) {
      val topicNameNew = replaceSpaces(topicName)
      println(s"Adding '$topicNameNew' for creation================")
      newTopics.add(new NewTopic(topicNameNew, 3, 1.toShort))
    }
    println("Creating topics================")
    adminClient.createTopics(newTopics)
    adminClient.close()
    println("Topics created successfully================")
  }

  def replaceSpaces(topicName: String): String = {
    topicName.trim.split(" ").mkString("_").trim
  }
}
