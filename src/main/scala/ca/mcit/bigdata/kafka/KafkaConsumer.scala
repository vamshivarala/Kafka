package ca.mcit.bigdata.kafka

import java.time.Duration

import org.apache.kafka.clients.consumer.ConsumerConfig._
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object KafkaConsumer extends App {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-2")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[String, String](consumerProperties)
  consumer.subscribe(List("trips").asJava)

  while (true) {

    val polledRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      var enrichedList: List[EnrichedTrip] = List()
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record: ConsumerRecord[String, String] = recordIterator.next()
        val csvTrip = record.value()

        val enrichedTrip = EnrichedTrip(Trip(record.value()), null, 0, null, null)
        enrichedList = enrichedList ++ List(enrichedTrip)
      }
      ProduceEnriched.produceEnrichedTopic(enrichedList)
    }
  }
}
