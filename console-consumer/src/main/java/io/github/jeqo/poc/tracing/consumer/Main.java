package io.github.jeqo.poc.tracing.consumer;

import brave.Tracing;
import brave.sampler.Sampler;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.typesafe.config.ConfigFactory;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
  public static void main(String[] args) {
    var config = ConfigFactory.load();
    // Instrument
    var sender =
        URLConnectionSender.newBuilder().endpoint(config.getString("zipkin.endpoint")).build();
    var reporter = AsyncReporter.builder(sender).build();
    var tracing = Tracing.newBuilder().localServiceName("console-consumer")
        .sampler(Sampler.ALWAYS_SAMPLE).spanReporter(reporter).traceId128Bit(true).build();
    var promRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    // Run application
    Server server = new ServerBuilder()
        .http(8083)
        .service("/metrics", (ctx, req) ->
            HttpResponse.of(MediaType.PLAIN_TEXT_UTF_8, promRegistry.scrape()))
        .build();
    server.start();
    var consoleConsumer = new ConsoleConsumer(tracing, promRegistry, config);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      server.close();
      reporter.close();
      tracing.close();
      promRegistry.close();
      consoleConsumer.stop();
    }));
    consoleConsumer.run();
  }
}
