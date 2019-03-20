package io.github.jeqo.poc.tracing.producer.metadata;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class MetadataPublisher {

  final String topic;

  final Producer<String, String> kafkaProducer;

  MetadataPublisher(Tracing tracing, Config config) {
    var producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.getString("bootstrap-servers"));
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    topic = config.getString("topics.metadata");

    var kafkaTracing = KafkaTracing.newBuilder(tracing).writeB3SingleFormat(true).build();
    kafkaProducer = kafkaTracing.producer(new KafkaProducer<>(producerConfig));
  }

  void publish() throws Exception {
    var record = new ProducerRecord<>(topic, "A", "B");
    kafkaProducer.send(record).get();
  }
}
