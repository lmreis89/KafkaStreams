package co.enear.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, StringSerializer, LongSerializer, StringDeserializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.{KeyValueMapper, ValueMapper, KStreamBuilder, KStream}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.lang.{Iterable => JIterable}

import scala.concurrent.Await

/**
  * Created by luis on 17-03-2016.
  */
object WordCount extends App {
  import StreamImplicits._

  val settings = new Properties()

  settings.put(StreamsConfig.JOB_ID_CONFIG, "kafka-streams-test")
  settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
  settings.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  settings.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  settings.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  settings.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

  val config = new StreamsConfig(settings)

  val stringSerializer = new StringSerializer
  val stringDeserializer = new StringDeserializer
  val longSerializer = new LongSerializer
  val longDeserializer = new LongDeserializer

  val builder = new KStreamBuilder()

  val textLines: KStream[String, String] = builder.stream("LinesTopic")

  val wordCounts = textLines
    .flatMapValues[String] { s: String =>  println("HUEHUEUUHE"); s.toLowerCase.split("\\W+") }
    .map { (key, value) => new KeyValue(value, value) }
    .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts")
    .toStream

  wordCounts.to("words-with-count", stringSerializer, longSerializer)

  val streams = new KafkaStreams(builder, config)

  streams.start()

  val producerConfig = new Properties();
  producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
  producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val inputValues = List("This is Kafka", "This is Luis")

  val producer = new KafkaProducer[Nothing, String](producerConfig)
  inputValues foreach { value =>
    val record = new ProducerRecord[Nothing, String]("kafka-streams-test", value)
    val f = producer.send(record)
    f.get()
  }

  producer.flush()
  producer.close()

  // Give the streaming job some time to do its work.
  // Note: The sleep times are relatively high to support running the build on Travis CI.
  Thread.sleep(10000)
  streams.close()
}

object StreamImplicits {
  implicit def arrayToIterable[T](a: Array[T]): JIterable[T] = {
    a.toIterable.asJava
  }

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue[K,V](tuple._1, tuple._2)
}

object ConsumerRead extends App {
  val consumerConfig = new Properties()
  consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "map-function-lambda-integration-test-standard-consumer")
  consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer(consumerConfig)
  consumer.subscribe(java.util.Collections.singletonList("words-with-count"))
  val actualValues = IntegrationTestUtils.readValues(consumer, 15)
  println("ACTUAL VALUES:")
  println(actualValues.size)
  println(actualValues)
}
