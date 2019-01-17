package com.expedia.www.haystack;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import lombok.Builder;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

/**
 * This object will turn a full fledge span into a skinny one and tee it off
 * to its own reporter.
 *
 * The idea is to create a full fidelity stream of lightweight spans for
 * data derivation outside of just tracing.
 */
public class SkeletalSpanFinishedHandler extends FinishedSpanHandler {

    private String localServiceName;
    private Reporter<Span> delegate;

    @Builder
    SkeletalSpanFinishedHandler(String localserviceName, Reporter<Span> delegate){
        this.localServiceName = localserviceName;
        this.delegate = delegate;
    }

    @java.lang.Override
    public boolean handle(TraceContext traceContext, MutableSpan mutableSpan) {
        if (mutableSpan.kind() == null) return false; // skip local spans

        zipkin2.Span.Builder builder = zipkin2.Span.newBuilder()
                .traceId(traceContext.traceIdString())
                .parentId(traceContext.isLocalRoot() ? null : traceContext.localRootIdString()) // rewrite the parent ID
                .id(traceContext.spanIdString())
                .name(mutableSpan.name())
                .kind(zipkin2.Span.Kind.valueOf(mutableSpan.kind().name()))
                .localEndpoint(Endpoint.newBuilder().serviceName(localServiceName).build());

        if (mutableSpan.error() != null || mutableSpan.tag("error") != null) {
            builder.putTag("error", ""); // linking counts errors: the value isn't important
        }
        if (mutableSpan.remoteServiceName() != null) {
            builder.remoteEndpoint(Endpoint.newBuilder().serviceName(mutableSpan.remoteServiceName()).build());
        }

        delegate.report(builder.build());
        return false; // end of the line
    }

}
