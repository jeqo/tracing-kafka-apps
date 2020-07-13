package poc.tracing.producer.event;

import static java.util.stream.Collectors.toMap;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.messaging.MessagingTracing;
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
import org.apache.kafka.clients.producer.ProducerConfig;
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
        var tracing = Tracing.newBuilder().localServiceName("http-event-producer")
                .sampler(Sampler.ALWAYS_SAMPLE)
                .traceId128Bit(true)
                .currentTraceContext(RequestContextCurrentTraceContext.ofDefault())
                .addSpanHandler(handler)
                .build();

        var producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString("bootstrap-servers"));
        //producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 1_000);
        //producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 100000);
        var customConfig = config.getConfig("kafka-producer.properties").entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().unwrapped()));
        producerConfig.putAll(customConfig);

        var topic = config.getString("topics.events");

        var messagingTracing = MessagingTracing.newBuilder(tracing).build();
        var kafkaTracing = KafkaTracing.newBuilder(messagingTracing).build();
        var kafkaProducer = kafkaTracing.producer(
                new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer()));
        // Service
        var eventPublisher = new EventPublisher(kafkaProducer, topic);
        //HTTP Server
        Server server = Server.builder()
                .http(8080)
                .decorator(BraveService.newDecorator(tracing))
                .service("/", (ctx, req) -> {
                    Tracer tracer = Tracing.currentTracer();
                    ScopedSpan span = tracer.startScopedSpan("process");
                    try {
                        span.tag("scenario", "producer-async-send");
                        eventPublisher.publish();
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
