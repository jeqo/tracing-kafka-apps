package io.github.jeqo.poc.tracing.processor;

import brave.Tracing;
import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.CurrentTraceContext;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.propagation.TraceContext;
import brave.sampler.RateLimitingSampler;
import brave.sampler.Sampler;
import com.typesafe.config.ConfigFactory;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
  public static void main(String[] args) {
    var config = ConfigFactory.load();
    //Instrument
    var sender =
        URLConnectionSender.newBuilder().endpoint(config.getString("zipkin.endpoint")).build();
    var reporter = AsyncReporter.builder(sender).build();
    var tracing = Tracing.newBuilder()
        .localServiceName("stream-processor-joiner")
        .sampler(Sampler.ALWAYS_SAMPLE)
        .spanReporter(reporter)
        .alwaysReportSpans()
        .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder().build())
        .addFinishedSpanHandler(new FinishedSpanHandler() {
          @Override public boolean handle(TraceContext context, MutableSpan span) {
            return true;
          }
          @Override public boolean alwaysSampleLocal() {
            return true;
          }
        })
        .traceId128Bit(true)
        .build();
    // Run application
    var streamProcessorJoiner = new StreamProcessorJoiner(tracing, config);
    var streams = streamProcessorJoiner.kafkaStreams();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    streams.start();
  }
}
