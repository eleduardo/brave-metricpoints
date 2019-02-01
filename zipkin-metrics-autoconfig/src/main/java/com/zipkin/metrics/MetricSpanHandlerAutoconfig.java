package com.zipkin.metrics;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class MetricSpanHandlerAutoconfig {
    @Bean
    public MetricSpanFinishedHandler haystackHandler(@Value("${spring.zipkin.service.name:${spring.application.name:default}}") String serviceName,
                                                     @Value("${spring.zipking.metrics.endpoint}") String endpoint){
        //TODO we can be more sofisticated with the reporter but keep it easy for now
        return MetricSpanFinishedHandler.newBuilder()
                .endpoint(endpoint)
                .localServiceName(serviceName)
                .build();
    }
}
