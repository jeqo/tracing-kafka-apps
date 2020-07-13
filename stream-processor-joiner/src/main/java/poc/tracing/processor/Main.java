package poc.tracing.processor;

import static java.util.stream.Collectors.toMap;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import brave.sampler.Sampler;
import com.typesafe.config.ConfigFactory;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
    public static void main(String[] args) {
        var config = ConfigFactory.load();
        //Instrument
        var sender = URLConnectionSender.newBuilder()
                .endpoint(config.getString("zipkin.endpoint"))
                .build();
        var handler = AsyncZipkinSpanHandler.create(sender);
        var tracing = Tracing.newBuilder()
                .localServiceName("stream-processor-joiner")
                .sampler(Sampler.ALWAYS_SAMPLE)
                .addSpanHandler(handler)
                .traceId128Bit(true)
                .build();
        // Run application
        var eventTopic = config.getString("topics.events");
        var metadataTopic = config.getString("topics.metadata");
        var enrichedEventTopic = config.getString("topics.enriched-events");

        var kafkaStreamsTracing = KafkaStreamsTracing.create(tracing);

        var streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString("bootstrap-servers"));
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application-id"));
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, config.getString("state-dir"));
        streamsConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        var customConfig = config.getConfig("kafka-streams.properties").entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().unwrapped()));
        streamsConfig.putAll(customConfig);

        var streamProcessorJoiner = new StreamProcessorJoiner(
                eventTopic, metadataTopic, enrichedEventTopic);
        var topology = streamProcessorJoiner.topology();
        var streams = kafkaStreamsTracing.kafkaStreams(topology, streamsConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            tracing.close();
            handler.close();
            sender.close();
        }));
        streams.start();
    }
}
