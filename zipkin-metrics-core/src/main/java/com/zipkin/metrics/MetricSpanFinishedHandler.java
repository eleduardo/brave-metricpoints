package com.zipkin.metrics;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import java.io.Closeable;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * This finish handler will skin down spans to the bare minimum needed to produce operational
 * metrics out of the spans. It will send the span to the collection agent regardless of
 * sampling, the proposed agent at this point is 'pitchfork'. The agent should be separate from the
 * regular tracing collection point although it follows the same API and uses the same reporter.
 */
public final class MetricSpanFinishedHandler extends FinishedSpanHandler implements Closeable {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    String localServiceName, endpoint;

    AsyncReporter<Span> reporter = null;

    public final Builder localServiceName(String localServiceName) {
      if (localServiceName == null) throw new NullPointerException("localServiceName == null");
      this.localServiceName = localServiceName;
      return this;
    }

    public final Builder endpoint(String endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.endpoint = endpoint;
      return this;
    }

    public final Builder reporter(AsyncReporter<Span> reporter){
      if (reporter==null) throw new NullPointerException("reporter == null");
      this.reporter = reporter;
      return this;
    }

    public final MetricSpanFinishedHandler build() {
      return new MetricSpanFinishedHandler(this);
    }

    Builder() {
    }
  }

  final String localServiceName;
  final AsyncReporter<Span> spanReporter;

  MetricSpanFinishedHandler(Builder builder) {
    if (builder.localServiceName == null) {
      throw new NullPointerException("localServiceName == null");
    }
    this.localServiceName = builder.localServiceName;
    if (builder.reporter != null){
      this.spanReporter = builder.reporter;
    }else {
      if (builder.endpoint == null){
        throw new NullPointerException("endpoint == null");
      }
      this.spanReporter = AsyncReporter.create(URLConnectionSender.create(builder.endpoint));
    }
  }

  @Override public boolean alwaysSampleLocal() {
    return true;
  }

  @Override public boolean handle(TraceContext traceContext, MutableSpan mutableSpan) {
    zipkin2.Span.Builder builder = zipkin2.Span.newBuilder()
        .traceId(traceContext.traceIdString())
        .parentId(traceContext.parentIdString())
        .id(traceContext.spanIdString())
        .shared(mutableSpan.shared())
        .name(mutableSpan.name())
        .timestamp(mutableSpan.startTimestamp())
        .duration(mutableSpan.finishTimestamp() - mutableSpan.startTimestamp())
        .localEndpoint(Endpoint.newBuilder().serviceName(localServiceName).build());

    if (mutableSpan.kind() != null) {
      builder.kind(zipkin2.Span.Kind.valueOf(mutableSpan.kind().name()));
    }
    if (mutableSpan.error() != null || mutableSpan.tag("error") != null) {
      builder.putTag("error", ""); // linking counts errors: the value isn't important
    }
    if (mutableSpan.remoteServiceName() != null) {
      builder.remoteEndpoint(
          Endpoint.newBuilder().serviceName(mutableSpan.remoteServiceName()).build());
    }

    spanReporter.report(builder.build());
    return true; // allow normal zipkin to accept the same span
  }

  @Override public void close() {
    spanReporter.close();
  }
}
