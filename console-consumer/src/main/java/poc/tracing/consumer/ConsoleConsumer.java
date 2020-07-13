package poc.tracing.consumer;

import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class ConsoleConsumer implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(ConsoleConsumer.class);

    final Properties config;
    final Collection<String> topics;
    final AtomicBoolean running = new AtomicBoolean(true);

    final KafkaTracing kafkaTracing;
    final Tracer tracer;

    public ConsoleConsumer(Properties config, Collection<String> topics) {
        this.config = config;
        this.topics = topics;

        final var tracing = Tracing.current();
        this.tracer = tracing.tracer();
        this.kafkaTracing = KafkaTracing.newBuilder(tracing).build();
    }

    private void printRecord(ConsumerRecord<String, String> record) {
        var span = kafkaTracing.nextSpan(record).name("print").start();
        try (var ws = tracer.withSpanInScope(span)) {
            var ts = Stream.of(record.headers().toArray())
                    .map(h -> String.format("%s:%s", h.key(), Arrays.toString(h.value())))
                    .collect(toList());
            String headers = String.join(",", ts);
            System.out.printf("record %s-%s-%s: %s=%s (headers: %s)%n", record.topic(),
                    record.partition(), record.offset(), record.key(), record.value(), headers);
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    @Override
    public void run() {
        LOG.info("Starting Console consumer");
        try (Consumer<String, String> consumer = kafkaTracing.consumer(new KafkaConsumer<>(config))) {
            consumer.subscribe(topics);
            while (running.get()) {
                var records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    printRecord(record);
                    //consumer.commitSync(
                    //    Map.of(
                    //        new TopicPartition(record.topic(), record.partition()),
                    //        new OffsetAndMetadata(record.offset())));
                });
                //consumer.commitSync();
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
