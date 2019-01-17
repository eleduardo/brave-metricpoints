package com.expedia.www.haystack;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import java.io.Closeable;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * This object will turn a full fledge span into a skinny one and tee it off to its own reporter.
 *
 * The idea is to create a full fidelity stream of lightweight spans for data derivation outside of
 * just tracing.
 */
public final class LightSpanFinishedHandler extends FinishedSpanHandler implements Closeable {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    String localServiceName, endpoint;

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

    public final LightSpanFinishedHandler build() {
      return new LightSpanFinishedHandler(this);
    }

    Builder() {
    }
  }

  final String localServiceName;
  final AsyncReporter<Span> asyncReporter;

  LightSpanFinishedHandler(Builder builder) {
    if (builder.localServiceName == null) {
      throw new NullPointerException("localServiceName == null");
    }
    this.localServiceName = builder.localServiceName;
    this.asyncReporter = AsyncReporter.create(URLConnectionSender.create(builder.endpoint));
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

    asyncReporter.report(builder.build());
    return true; // allow normal zipkin to accept the same span
  }

  @Override public void close() {
    asyncReporter.close();
  }
}
