package poc.tracing.producer.metadata;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.sampler.Sampler;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.brave.RequestContextCurrentTraceContext;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.brave.BraveService;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
    public static void main(String[] args) {
        var config = ConfigFactory.load();
        //Instrumentation
        var sender = URLConnectionSender.newBuilder()
                .endpoint(config.getString("zipkin.endpoint"))
                .build();
        var handler = AsyncZipkinSpanHandler.create(sender);
        var tracing = Tracing.newBuilder().localServiceName("http-metadata-producer")
                .sampler(Sampler.ALWAYS_SAMPLE)
                .traceId128Bit(true)
                .currentTraceContext(RequestContextCurrentTraceContext.ofDefault())
                .addSpanHandler(handler)
                .build();
        var producerConfig = new Properties();
        producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap-servers"));
        var customConfig = config.getConfig("kafka-producer.properties").entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().unwrapped()));
        producerConfig.putAll(customConfig);

        var topic = config.getString("topics.metadata");

        var kafkaTracing = KafkaTracing.newBuilder(tracing).build();
        var kafkaProducer = kafkaTracing.producer(
                new KafkaProducer<>(
                        producerConfig,
                        new StringSerializer(),
                        new StringSerializer()));
        // Service
        var metadataPublisher = new MetadataPublisher(kafkaProducer, topic);
        //HTTP Server
        var server = Server.builder()
                .http(18080)
                .decorator(BraveService.newDecorator(tracing))
                .service("/", (ctx, req) -> {
                    var tracer = Tracing.currentTracer();
                    var span = tracer.startScopedSpan("process");
                    try {
                        metadataPublisher.publish();
                        return HttpResponse.of(
                                HttpStatus.ACCEPTED,
                                MediaType.PLAIN_TEXT_UTF_8,
                                Instant.now().toString());
                    } catch (Exception e) {
                        span.error(e);
                        return HttpResponse.of(
                                HttpStatus.INTERNAL_SERVER_ERROR,
                                MediaType.PLAIN_TEXT_UTF_8,
                                e.getMessage());
                    } finally {
                        span.finish();
                    }
                })
                .build();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            tracing.close();
            handler.close();
            sender.close();
        }));
        server.start().join();
    }
}
