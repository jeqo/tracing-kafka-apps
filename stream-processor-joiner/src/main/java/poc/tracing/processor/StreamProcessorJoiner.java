package poc.tracing.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

class StreamProcessorJoiner {

    final String eventTopic;
    final String metadataTopic;
    final String enrichedEventTopic;


    StreamProcessorJoiner(String eventTopic, String metadataTopic, String enrichedEventTopic) {
        this.eventTopic = eventTopic;
        this.metadataTopic = metadataTopic;
        this.enrichedEventTopic = enrichedEventTopic;
    }

    Topology topology() {
        var builder = new StreamsBuilder();

        KTable<String, String> metadataTable =
                builder.table(metadataTopic, Materialized.as(Stores.inMemoryKeyValueStore(metadataTopic)));

        ValueJoiner<String, String, String> joiner =
                (event, metadata) -> String.format("%s (meta: %s)", event, metadata);

        builder.<String, String>stream(eventTopic)
                .join(metadataTable, joiner)
                .to(enrichedEventTopic);

        return builder.build();
    }

//    Topology otherTopology() {
//        var builder = new StreamsBuilder();
//
//        builder
//                .addStateStore(
//                        Stores.keyValueStoreBuilder(
//                                Stores.persistentKeyValueStore(metadataTopic),
//                                Serdes.String(),
//                                Serdes.String()))
//                .<String, String>stream(metadataTopic)
//                .process(() -> new Processor<>() {
//                    KeyValueStore<String, String> store;
//
//                    @Override
//                    public void init(ProcessorContext context) {
//                        store = (KeyValueStore<String, String>) context.getStateStore(metadataTopic);
//                    }
//
//                    @Override
//                    public void process(String key, String value) {
//                        store.put(key, value);
//                    }
//
//                    @Override
//                    public void close() {
//                    }
//                }, metadataTopic);
//
//        builder
//                .<String, String>stream(eventTopic)
//                .transform(
//                        () -> new Transformer<String, String, KeyValue<String, String>>() {
//                            KeyValueStore<String, String> store;
//
//                            @Override
//                            public void init(ProcessorContext context) {
//                                store = (KeyValueStore<String, String>) context.getStateStore(metadataTopic);
//                            }
//
//                            @Override
//                            public KeyValue<String, String> transform(String key, String value) {
//                                String metadata = store.get(key);
//                                return KeyValue.pair(key, String.format("%s (meta: %s)", value, metadata));
//                            }
//
//                            @Override
//                            public void close() {
//                            }
//                        }, metadataTopic)
//                .to(enrichedEventTopic, Produced.with(Serdes.String(), Serdes.String()));
//
//        return builder.build();
//    }
}
