package com.expedia.www.haystack;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;

public class MetricPointSpanFinisher extends FinishedSpanHandler {

    @Override
    public boolean handle(TraceContext traceContext, MutableSpan mutableSpan) {
        return false;
    }
}
