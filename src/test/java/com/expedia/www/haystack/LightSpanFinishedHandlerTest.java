package com.expedia.www.haystack;

import brave.ScopedSpan;
import brave.Span;
import brave.Span.Kind;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.B3SingleFormat;
import brave.propagation.TraceContext;
import brave.sampler.Sampler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import zipkin2.DependencyLink;
import zipkin2.internal.DependencyLinker;
import zipkin2.junit.ZipkinRule;
import zipkin2.reporter.Reporter;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class LightSpanFinishedHandlerTest {
  @Rule public ZipkinRule http = new ZipkinRule();

  LightSpanFinishedHandler handler1 = LightSpanFinishedHandler.newBuilder()
      .localServiceName("server1")
      .endpoint(http.httpUrl() + "/api/v2/spans").build();

  LightSpanFinishedHandler handler2 = LightSpanFinishedHandler.newBuilder()
      .localServiceName("server2")
      .endpoint(http.httpUrl() + "/api/v2/spans").build();

  Tracer server1Tracer = Tracing.newBuilder()
      .localServiceName(handler1.localServiceName)
      .addFinishedSpanHandler(handler1)
      .sampler(Sampler.NEVER_SAMPLE) // prove that the light one is always recording
      .spanReporter(Reporter.NOOP)
      .build().tracer();

  Tracer server2Tracer = Tracing.newBuilder()
      .localServiceName(handler2.localServiceName)
      .addFinishedSpanHandler(handler2)
      .sampler(Sampler.NEVER_SAMPLE) // prove that the light one is always recording
      .spanReporter(Reporter.NOOP)
      .build().tracer();

  @Before public void acceptTwoServerRequests() {
    acceptTwoServerRequests(server1Tracer, server2Tracer);
    handler1.close();
    handler2.close();
  }

  @After public void close() {
    Tracing.current().close();
  }

  @Test public void includesAllSpanNames() {
    assertThat(http.getTraces())
        .extracting(s -> s.stream().map(zipkin2.Span::name).sorted().collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            asList("controller", "get", "post", "post"),
            asList("async1"),
            asList("async2"),
            asList("controller2", "get", "post", "post", "post")
        );
  }

  @Test public void skeletalSpans_produceSameServiceGraph() {
    assertThat(link(http.getTraces()))
        .containsExactlyInAnyOrder(
            DependencyLink.newBuilder()
                .parent("server1")
                .child("server2")
                .callCount(2L)
                .errorCount(1L)
                .build(),
            DependencyLink.newBuilder()
                .parent("server1")
                .child("uninstrumentedserver")
                .callCount(1L)
                .errorCount(1L)
                .build()
        );
  }

  /** Executes the linker for each collected trace */
  static List<DependencyLink> link(List<List<zipkin2.Span>> spans) {
    DependencyLinker linker = new DependencyLinker();
    spans.forEach(linker::putTrace);
    return linker.link();
  }

  /** Simulates some service calls */
  static void acceptTwoServerRequests(Tracer server1Tracer, Tracer server2Tracer) {
    Span server1 = server1Tracer.newTrace().name("get").kind(Kind.SERVER).start();
    Span server2 = server1Tracer.newTrace().name("get").kind(Kind.SERVER).start();
    try {
      Span client1 =
          server1Tracer.newChild(server1.context()).name("post").kind(Kind.CLIENT).start();

      server2Tracer.joinSpan(fakeUseOfHeaders(client1.context()))
          .name("post")
          .kind(Kind.SERVER)
          .start().finish();

      ScopedSpan local1 = server1Tracer.startScopedSpanWithParent("controller", server1.context());
      try {
        try {
          server1Tracer.newTrace().name("async1").start().finish();
          server2Tracer.newTrace().name("async2").start().finish();

          ScopedSpan local2 =
              server1Tracer.startScopedSpanWithParent("controller2", server2.context());
          Span client2 = server1Tracer.nextSpan().name("post").kind(Kind.CLIENT).start();
          try {
            server2Tracer.joinSpan(fakeUseOfHeaders(client2.context()))
                .name("post")
                .kind(Kind.SERVER)
                .start().error(new RuntimeException()).finish();

            server1Tracer.nextSpan()
                .name("post")
                .kind(Kind.CLIENT)
                .start()
                .remoteServiceName("uninstrumentedServer")
                .error(new RuntimeException())
                .finish();
          } finally {
            client2.finish();
            local2.finish();
          }
        } finally {
          server2.finish();
        }
      } finally {
        client1.finish();
        local1.finish();
      }
    } finally {
      server1.finish();
    }
  }

  /** Ensures reporting is partitioned by trace ID */
  static Reporter<zipkin2.Span> toReporter(Map<String, List<zipkin2.Span>> spans) {
    return s -> spans.computeIfAbsent(s.traceId(), k -> new ArrayList<>()).add(s);
  }

  /** To reduce code, we don't use real client-server instrumentation. This fakes headers. */
  static TraceContext fakeUseOfHeaders(TraceContext context) {
    return B3SingleFormat.parseB3SingleFormat(B3SingleFormat.writeB3SingleFormat(context))
        .context();
  }
}
