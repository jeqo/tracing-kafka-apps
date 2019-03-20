package io.github.jeqo.poc.tracing.producer.event;

import brave.Tracing;
import brave.sampler.Sampler;
import com.typesafe.config.ConfigFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class Main {
  public static void main(String[] args) throws Exception {
    var config = ConfigFactory.load();
    //Instrumentation
    var sender =
        URLConnectionSender.newBuilder().endpoint(config.getString("zipkin.endpoint")).build();
    var reporter = AsyncReporter.builder(sender).build();
    var tracing = Tracing.newBuilder().localServiceName("http-event-producer")
        .sampler(Sampler.ALWAYS_SAMPLE)
        .traceId128Bit(true)
        .spanReporter(reporter)
        .build();
    // Service
    var eventPublisher = new EventPublisher(Tracing.current(), ConfigFactory.load());
    //HTTP Server
    Server server = new Server(8080);
    ServletHandler servletHandler = new ServletHandler();
    HttpEventServlet servlet = new HttpEventServlet(tracing, eventPublisher);
    servletHandler.addServletWithMapping(new ServletHolder(servlet), "/*");
    server.setHandler(servletHandler);
    Runtime.getRuntime().addShutdownHook(new Thread(server::destroy));
    server.start();
    server.join();
  }
}
