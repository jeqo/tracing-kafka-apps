package io.github.jeqo.poc.tracing.producer.metadata;

import brave.Tracer;
import brave.Tracing;
import java.io.IOException;
import java.time.Instant;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HttpMetadataServlet extends HttpServlet {
  final MetadataPublisher metadataPublisher;
  final Tracer tracer;

  HttpMetadataServlet(Tracing tracing, MetadataPublisher metadataPublisher) {
    this.metadataPublisher = metadataPublisher;
    tracer = tracing.tracer();
  }

  @Override protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    var span = tracer.startScopedSpan("process");
    try {
      metadataPublisher.publish();
      resp.setStatus(HttpServletResponse.SC_ACCEPTED);
      resp.getWriter().println(Instant.now().toString());
    } catch (Exception e) {
      span.error(e);
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resp.getWriter().println(e.getMessage());
    } finally {
      span.finish();
    }
  }
}
