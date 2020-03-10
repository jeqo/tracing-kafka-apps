package io.github.jeqo.poc.tracing.processor;

import brave.Tracing;
import brave.sampler.Sampler;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
  public static void main(String[] args) {
    var config = ConfigFactory.load();
    //Instrument
    var sender =
        URLConnectionSender.newBuilder().endpoint(config.getString("zipkin.endpoint")).build();
    var reporter = AsyncReporter.builder(sender).build();
    var tracing = Tracing.newBuilder().localServiceName("stream-processor-joiner")
        .sampler(Sampler.ALWAYS_SAMPLE).spanReporter(reporter).traceId128Bit(true).build();
    var promRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    // Run application
    var streamProcessorJoiner = new StreamProcessorJoiner(tracing, config);
    var streams = streamProcessorJoiner.kafkaStreams();
    new KafkaStreamsMetrics(streams).bindTo(promRegistry);
    var server = Server.builder()
        .http(8082)
        .service("/metrics", (ctx, req) ->
            HttpResponse.of(MediaType.PLAIN_TEXT_UTF_8, promRegistry.scrape()))
        .build();
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      tracing.close();
      reporter.close();
      promRegistry.close();
      server.close();
      streams.close();
    }));
    streams.start();
  }
}
