package co.enear.kafka

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}

/**
  * Created by luis on 18-03-2016.
  */
object UpperCaseMapper extends App {
  import StreamImplicits._

  val settings = new Properties()

  settings.put(StreamsConfig.JOB_ID_CONFIG, "kafka-streams-fds")
  settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
  settings.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  settings.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  settings.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  settings.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  settings.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val config = new StreamsConfig(settings)

  val stringSerializer = new StringSerializer
  val stringDeserializer = new StringDeserializer

  val builder = new KStreamBuilder()

  builder.stream[String, String]("upper-fds")
    .map[String, String]((key, value) => (key, value.toUpperCase()))
    .to("fds-result", stringSerializer, stringSerializer)

  val stream: KafkaStreams = new KafkaStreams(builder, config)
  stream.start()

  Thread.sleep(30000)

  stream.close()

}
