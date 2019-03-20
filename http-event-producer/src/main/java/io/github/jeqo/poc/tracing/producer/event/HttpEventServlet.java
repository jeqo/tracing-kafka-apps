package io.github.jeqo.poc.tracing.producer.event;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import java.io.IOException;
import java.time.Instant;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HttpEventServlet extends HttpServlet {
  final Tracer tracer;
  final EventPublisher eventPublisher;

  HttpEventServlet(Tracing tracing, EventPublisher eventPublisher) {
    this.eventPublisher = eventPublisher;
    tracer = tracing.tracer();
  }

  @Override protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    ScopedSpan span = tracer.startScopedSpan("process");
    try {
      span.tag("scenario", "consumer-sync-commit-per-record");
      eventPublisher.publish();
      resp.setContentType("text/plain; utf-8");
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
