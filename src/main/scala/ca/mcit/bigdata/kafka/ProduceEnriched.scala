package ca.mcit.bigdata.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object ProduceEnriched extends App {

  def produceEnrichedTopic(enrichedList: List[EnrichedTrip]): Unit = {
    val producerProperties = new Properties()
    producerProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
    )
    producerProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )
    producerProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )
    val producer = new KafkaProducer[String, String](producerProperties)
    val topic = "enriched_trip"
    for (list <- enrichedList) {
      val csvList = EnrichedTrip.toCsv(list)
      producer.send(new ProducerRecord[String, String](topic, csvList))
    }
    producer.flush()
  }
}

