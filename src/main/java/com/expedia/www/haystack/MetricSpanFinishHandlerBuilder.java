package com.expedia.www.haystack;

import brave.handler.FinishedSpanHandler;
import java.io.Closeable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

@Slf4j
public class MetricSpanFinishHandlerBuilder implements Closeable {

    private String localServiceName;
    private AsyncReporter<Span> reporter;

    @Builder
    private MetricSpanFinishHandlerBuilder(String url, String localServiceName, Sender sender){
        if (localServiceName == null || localServiceName.trim().isEmpty()){
            log.error("A local service name is required");
            throw new IllegalArgumentException("A proper URL needs to be configured for the reporter to send spans");
        }
        this.localServiceName = localServiceName;

        //initialize the sender
        Sender selectedsender = sender;
        if (selectedsender == null) {
            if (url == null || url.trim().isEmpty()) {
                log.error("A proper URL needs to be configured for the reporter to send spans");
                throw new IllegalArgumentException("A proper URL needs to be configured for the reporter to send spans");
            }

            selectedsender = URLConnectionSender.newBuilder().endpoint(url).build();
        }

        this.reporter = AsyncReporter.builder(selectedsender).build();
    }

    public FinishedSpanHandler build(){
        return new SkeletalSpanFinishedHandler(this.localServiceName, this.reporter);
    }

    @Override public void close() {
        this.reporter.close();
    }
}
