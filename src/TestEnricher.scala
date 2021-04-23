package ca.mcit.bigdata.kafka

  import java.time.Duration
  import java.util.Properties

  import org.apache.kafka.clients.consumer.ConsumerConfig._
  import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
  import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}

  import scala.collection.JavaConverters._

  object TestEnricher extends App {

    val consumerProperties = new Properties()
    consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-2") //sometimes you have to use a different user ID to force it to read from beginning.  Found this in Spark project.  Not sure why
    consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

    val consumer = new KafkaConsumer[Int, String](consumerProperties)
    consumer.subscribe(List("enriched_trip").asJava)

    println("| Key | Message | Partition | Offset |")
    while (true) {
      val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(5))  //the number of seconds to wait for new records if there are none, before returning empty record set
      if (!polledRecords.isEmpty) {
        println(s"Polled ${polledRecords.count()} records")
        val recordIterator = polledRecords.iterator()
        while (recordIterator.hasNext) {
          val record: ConsumerRecord[Int, String] = recordIterator.next()
          println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
        }
      }
    }


  }
