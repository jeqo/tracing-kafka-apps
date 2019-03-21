package io.github.jeqo.poc.tracing.consumer.demistify;

import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import com.typesafe.config.Config;
import io.github.jeqo.poc.tracing.consumer.ConsoleConsumer;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomKafkaConsumer implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(ConsoleConsumer.class);
  final Properties config;
  final Collection<String> topics;
  final KafkaTracing kafkaTracing;
  final Tracer tracer;

  public CustomKafkaConsumer(Tracing tracing, Config consumerConfig) {
    config = new Properties();
    config.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getString("bootstrap-servers"));
    config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerConfig.getString("group-id"));
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // specific case: will commit explicitly after record is processed
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    tracer = tracing.tracer();
    kafkaTracing = KafkaTracing.newBuilder(tracing).writeB3SingleFormat(true).build();

    topics = consumerConfig.getStringList("topics");
  }

  @Override
  public void run() {
    LOG.info("Starting Console consumer");
    Consumer<String, String> tracingConsumer = kafkaTracing.consumer(new KafkaConsumer<>(config));
    var consumerRebalanceListener = new CustomRebalanceListener((KafkaConsumer) tracingConsumer);
    try {
      tracingConsumer.subscribe(topics, consumerRebalanceListener);
      while (true) {
        // clear state of processed records for each poll
        consumerRebalanceListener.clearProcessedRecordsOffsets();
        var records = tracingConsumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> record : records) {
          processRecord(record, consumerRebalanceListener);
        }
        // if async, can be race condition between consumerRebalanceListener.clearProcessedRecordsOffsets();
        // todo: investigate possibility to use commitAsync()
        tracingConsumer.commitSync();
      }
    } catch (RuntimeException | Error e) {
      LOG.warn("Unexpected error in polling loop spans", e);
      throw e;
    } finally {
      LOG.info("Kafka consumer polling loop stopped. Kafka consumer trying commit last processed records");
      try {
        consumerRebalanceListener.commitProcessedRecords();
      } finally{
        LOG.info("Closing consumer");
        tracingConsumer.close();
      }
    }
  }

  private void processRecord(ConsumerRecord<String, String> record, CustomRebalanceListener customRebalanceListener) {
    // can be long-time processing
    customRebalanceListener.addOffset(record.topic(), record.partition(), record.offset());
  }
}
