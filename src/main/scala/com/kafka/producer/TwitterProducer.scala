package com.kafka.producer

import java.util
import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque, TimeUnit}

import com.kafka.producer.utils.Utilities
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, HttpHosts}
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

class TwitterProducer(var msgQueue: LinkedBlockingDeque[String],
                      var topicName: String,
                      var topicAlias: String)
  extends Thread {

  override def run(): Unit = {
    println(s"$topicAlias:: Creating twitter client================")
    val client = createTwitterClient(TwitterProducerMain.kafkaProperties, msgQueue)
    client.connect()
    println(s"$topicAlias:: Twitter client connected successfully================")

    println(s"$topicAlias:: Creating producer================")
    val producer = createKafkaProducer(Utilities.getProducerProperties(TwitterProducerMain.kafkaProperties))
    println(s"$topicAlias:: Producer created successfully================")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println(s"$topicAlias:: Producer shutdown hook called. Shutting down application================")
      client.stop()
      producer.close()
    }))

    while (!client.isDone) {
      var msg: String = null
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
          client.stop()
      }
      if (msg != null) {
        synchronized {
          println(s"$topicAlias:: $msg")
          println("Producing message================")
          producer.send(new ProducerRecord[String, String](topicAlias, null, msg),
            (metadata: RecordMetadata, exception: Exception) => {
              if (exception != null) {
                println(s"$topicAlias:: Failed to produce message: $exception\n================")
              } else {
                println(s"$topicAlias:: Message produced successfully================")
              }
            })
        }
      }
    }
    throw new RuntimeException("Producer terminated")
  }

  def createTwitterClient(twitterProperties: Properties, msgQueue: BlockingQueue[String]): Client = {
    synchronized {
      val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
      val hosebirdEndpoint = new StatusesFilterEndpoint
      hosebirdEndpoint.trackTerms(util.Arrays.asList(topicName))

      val hosebirdAuth = new OAuth1(twitterProperties.getProperty(Utilities.TWITTER_API_KEY_PROPERTY),
        twitterProperties.getProperty(Utilities.TWITTER_API_SECRET_PROPERTY),
        twitterProperties.getProperty(Utilities.TWITTER_TOKEN_PROPERTY),
        twitterProperties.getProperty(Utilities.TWITTER_TOKEN_SECRET_PROPERTY))

      val builder = new ClientBuilder().name(s"Hosebird-Client-$topicAlias")
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue))

      builder.build
    }
  }

  def createKafkaProducer(producerProperties: Properties): KafkaProducer[String, String] = {
    new KafkaProducer(producerProperties)
  }
}
