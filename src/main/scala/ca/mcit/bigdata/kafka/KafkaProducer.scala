package ca.mcit.bigdata.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.io.Source

object KafkaProducer extends App {

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
  val topicName = "trips"
  val dataPath = "data/"
  val dataSource = Source.fromFile(dataPath + "trips.csv.txt")

  dataSource
    .getLines().drop(1)
    .foreach(line => {
      producer.send(new ProducerRecord[String, String](topicName, line))
    }
    )
  dataSource.close()
  producer.flush()
  producer.close()
}
