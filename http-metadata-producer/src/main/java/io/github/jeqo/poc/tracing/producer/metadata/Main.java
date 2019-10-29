package io.github.jeqo.poc.tracing.producer.metadata;

import brave.Tracing;
import brave.sampler.Sampler;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.brave.RequestContextCurrentTraceContext;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.brave.BraveService;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
  public static void main(String[] args) {
    var config = ConfigFactory.load();
    //Instrumentation
    var sender =
        URLConnectionSender.newBuilder().endpoint(config.getString("zipkin.endpoint")).build();
    var reporter = AsyncReporter.builder(sender).build();
    var tracing = Tracing.newBuilder().localServiceName("http-metadata-producer")
        .sampler(Sampler.ALWAYS_SAMPLE)
        .traceId128Bit(true)
        .currentTraceContext(RequestContextCurrentTraceContext.ofDefault())
        .spanReporter(reporter)
        .build();
    // Service
    var metadataPublisher = new MetadataPublisher(Tracing.current(), ConfigFactory.load());
    //HTTP Server
    var server = Server.builder().http(8081)
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
    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    server.start().join();
  }
}
