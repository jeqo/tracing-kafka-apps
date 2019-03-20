package io.github.jeqo.poc.tracing.consumer;

import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class ConsoleConsumer implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(ConsoleConsumer.class);

  final Properties config;
  final Collection<String> topics;
  final KafkaTracing kafkaTracing;
  final Tracer tracer;
  final AtomicBoolean running = new AtomicBoolean(true);

  public ConsoleConsumer(Tracing tracing, Config consumerConfig) {
    config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        consumerConfig.getString("bootstrap-servers"));
    config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerConfig.getString("group-id"));
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    tracer = tracing.tracer();
    kafkaTracing = KafkaTracing.newBuilder(tracing).writeB3SingleFormat(true).build();
    topics = consumerConfig.getStringList("topics");
  }

  private void printRecord(Consumer<String, String> consumer,
      ConsumerRecord<String, String> record) {
    var span = kafkaTracing.nextSpan(record).name("print").start();
    try (var ws = tracer.withSpanInScope(span)) {
      var ts = Stream.of(record.headers().toArray())
          .map(h -> String.format("%s:%s", h.key(), Arrays.toString(h.value())))
          .collect(toList());
      String headers = String.join(",", ts);
      System.out.printf("record %s-%s-%s: %s=%s (headers: %s)%n", record.topic(),
          record.partition(), record.offset(), record.key(), record.value(), headers);
      consumer.commitSync(
          Map.of(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset())));
    } catch (RuntimeException | Error e) {
      span.error(e);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override public void run() {
    LOG.info("Starting Console consumer");
    try (Consumer<String, String> tracingConsumer =
             kafkaTracing.consumer(new KafkaConsumer<>(config))) {
      tracingConsumer.subscribe(topics);
      while (running.get()) {
        var records = tracingConsumer.poll(Duration.ofSeconds(1));
        records.forEach(r -> this.printRecord(tracingConsumer, r));
      }
    } catch (RuntimeException | Error e) {
      LOG.warn("Unexpected error in polling loop spans", e);
      throw e;
    } finally {
      LOG.info("Kafka consumer polling loop stopped. Kafka consumer closed.");
    }
  }

  void stop() {
    running.set(false);
  }
}
