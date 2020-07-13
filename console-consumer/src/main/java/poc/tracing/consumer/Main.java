package poc.tracing.consumer;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import brave.Tracing;
import brave.sampler.Sampler;
import com.typesafe.config.ConfigFactory;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
    public static void main(String[] args) {
        var config = ConfigFactory.load();
        // Instrument
        var sender = URLConnectionSender.newBuilder()
                .endpoint(config.getString("zipkin.endpoint"))
                .build();
        var handler = AsyncZipkinSpanHandler.create(sender);
        var tracing = Tracing.newBuilder()
                .localServiceName("console-consumer")
                .sampler(Sampler.ALWAYS_SAMPLE)
                .addSpanHandler(handler)
                .traceId128Bit(true)
                .build();
        // Run application
        var consumerConfig = new Properties();
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap-servers"));
        consumerConfig.put(GROUP_ID_CONFIG, config.getString("group-id"));
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        var customConfig = config.getConfig("kafka-consumer.properties").entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().unwrapped()));
        consumerConfig.putAll(customConfig);

        var topics = config.getStringList("topics");
        var consoleConsumer = new ConsoleConsumer(consumerConfig, topics);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consoleConsumer.stop();
            handler.close();
            sender.close();
            tracing.close();
        }));
        consoleConsumer.run();
    }
}
