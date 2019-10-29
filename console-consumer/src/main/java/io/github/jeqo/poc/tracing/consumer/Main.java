package io.github.jeqo.poc.tracing.consumer;

import brave.Tracing;
import brave.sampler.Sampler;
import com.typesafe.config.ConfigFactory;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
  public static void main(String[] args) {
    var config = ConfigFactory.load();
    // Instrument
    var sender =
        URLConnectionSender.newBuilder().endpoint(config.getString("zipkin.endpoint")).build();
    var reporter = AsyncReporter.builder(sender).build();
    var tracing = Tracing.newBuilder()
        .localServiceName("console-consumer")
        .sampler(Sampler.ALWAYS_SAMPLE)
        .spanReporter(reporter)
        .traceId128Bit(true)
        .build();
    // Run application
    var consoleConsumer = new ConsoleConsumer(tracing, config);
    Runtime.getRuntime().addShutdownHook(new Thread(consoleConsumer::stop));
    consoleConsumer.run();
  }
}
