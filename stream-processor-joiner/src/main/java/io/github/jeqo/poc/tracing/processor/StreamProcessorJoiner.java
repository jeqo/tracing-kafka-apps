package io.github.jeqo.poc.tracing.processor;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

class StreamProcessorJoiner {

  final String eventTopic;
  final String metadataTopic;
  final String enrichedEventTopic;
  final Properties streamsConfig;

  final KafkaStreamsTracing kafkaStreamsTracing;

  StreamProcessorJoiner(Tracing tracing, Config config) {
    eventTopic = config.getString("topics.events");
    metadataTopic = config.getString("topics.metadata");
    enrichedEventTopic = config.getString("topics.enriched-events");

    kafkaStreamsTracing = KafkaStreamsTracing.create(tracing);

    streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.getString("bootstrap-servers"));
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application-id"));
    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, config.getString("state-dir"));
    streamsConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
  }

  KafkaStreams kafkaStreams() {
    return kafkaStreamsTracing.kafkaStreams(topology(), streamsConfig);
  }

  Topology topology() {
    var builder = new StreamsBuilder();

    var metadataTable =
        builder.table(
            metadataTopic,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(metadataTopic));

    ValueJoiner<String, String, String> joiner =
        (event, metadata) -> String.format("%s (meta: %s)", event, metadata);

    builder.<String, String>stream(eventTopic)
        .join(metadataTable, joiner)
        .to(enrichedEventTopic);

    return builder.build();
  }
}
