package com.telixia.reactivecoding;

import com.telixia.reactivecoding.feed.ReactiveMarketFeed;
import com.telixia.reactivecoding.model.MarketTick;
import com.telixia.reactivecoding.model.TraderOrder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LESSON 10 (CAPSTONE): Live Spring WebFlux Trading Blotter & Senior Architecture Defence
 *
 * LEARNING OBJECTIVES:
 * 1. Expose a non-blocking Netty HTTP API in Spring Boot 4.1 returning Mono<T> and Flux<T>.
 * 2. Stream real-time market ticks and order fills over Server-Sent Events (SSE) using
 *    MediaType.TEXT_EVENT_STREAM_VALUE—a prime use case where WebFlux shines over traditional REST.
 * 3. Use Reactor's thread-safe Sinks.Many (multicast().onBackpressureBuffer()) to bridge
 *    imperative POST order submissions into a live broadcast stream for all connected trader UIs.
 * 4. Articulate the Senior/Lead architectural comparison between Project Reactor (WebFlux)
 *    and Java 21/25 Virtual Threads in system-design interviews.
 *
 * ============================================================================================
 * SENIOR INTERVIEW DEFENCE: PROJECT REACTOR (WEBFLUX) vs. JAVA 25 VIRTUAL THREADS
 * ============================================================================================
 * 1. WHEN VIRTUAL THREADS (JAVA 21/25) WIN:
 *    - Standard request/response microservices backed by blocking JDBC, JPA/Hibernate, or legacy SDKs.
 *    - Virtual threads remove THREAD scarcity (unmounting from carrier threads on blocking I/O),
 *      preserving normal imperative stack traces, debuggers, and ScopedValue (final in Java 25).
 *    - Note: Java 24+ (JEP 491) eliminated synchronized monitor pinning, making JDK 25 even safer.
 *
 * 2. WHEN PROJECT REACTOR (WEBFLUX) STILL WINS (WHY THIS WORKSHOP EXISTS):
 *    - True end-to-end streaming with native backpressure (Subscription.request(n)): virtual
 *      threads have NO built-in backpressure protocol—you must build semaphores and bounded queues
 *      manually.
 *    - Tens of thousands of long-lived, mostly idle connections (SSE, WebSockets, RSocket) on a
 *      tiny Netty event-loop footprint (~1 thread per CPU core).
 *    - Complex temporal stream composition: windowing (VWAP), conflation (onBackpressureLatest),
 *      debounce/sample, and non-blocking parallel fan-out/fan-in pipelines.
 * ============================================================================================
 */
@SpringBootApplication
@RestController
@RequestMapping("/api/v1/trading")
public class Lesson10 {

    private final AtomicInteger idSequence = new AtomicInteger(100);

    /**
     * Thread-safe hot multicast sink broadcasting newly submitted orders to all connected
     * SSE blotter clients without dropping concurrent submissions.
     */
    private final Sinks.Many<TraderOrder> liveOrderBlotterSink =
            Sinks.many().multicast().onBackpressureBuffer(256, false);

    public static void main(String[] args) {
        SpringApplication.run(Lesson10.class, args);
    }

    /**
     * 1. REST Snapshot Endpoint (Mono):
     *    GET http://localhost:8080/api/v1/trading/orders/first?instrumentId=2
     */
    @GetMapping("/orders/first")
    public Mono<TraderOrder> getFirstRestingOrder(@RequestParam(defaultValue = "1") int instrumentId) {
        return ReactiveMarketFeed.findFirstOrderByInstrumentId(instrumentId);
    }

    /**
     * 2. Live Market-Data SSE Stream (Flux over text/event-stream):
     *    GET http://localhost:8080/api/v1/trading/ticks/stream
     *
     * WHY SSE + WEBFLUX:
     * Holds thousands of browser/terminal clients open on Netty's event loop with zero blocked
     * OS threads, automatically cancelling the upstream Flux when the client disconnects!
     */
    @GetMapping(value = "/ticks/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<MarketTick> streamLiveMarketTicks() {
        return ReactiveMarketFeed.liveLseTickStream(Duration.ofMillis(500))
                .onBackpressureLatest();
    }

    /**
     * 3. Live Order Blotter SSE Stream:
     *    GET http://localhost:8080/api/v1/trading/blotter/stream
     *    Merges initial resting orders with real-time orders submitted via POST /orders.
     */
    @GetMapping(value = "/blotter/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<TraderOrder> streamOrderBlotter() {
        return Flux.concat(
                ReactiveMarketFeed.orderBookFlux(),
                liveOrderBlotterSink.asFlux()
        );
    }

    /**
     * 4. Submit a New Limit Order (publishes into the live SSE blotter):
     *    POST http://localhost:8080/api/v1/trading/orders
     *    Body: {"traderName":"Nditah","desk":"QUANT_ALPHA","symbol":"LSEG.L","limitPriceMinorUnits":94200}
     */
    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<TraderOrder> submitOrder(@RequestBody OrderRequest request) {
        TraderOrder created = new TraderOrder(
                idSequence.incrementAndGet(),
                request.traderName(),
                request.desk(),
                request.symbol(),
                request.limitPriceMinorUnits()
        );
        liveOrderBlotterSink.tryEmitNext(created);
        return Mono.just(created);
    }

    public record OrderRequest(
            String traderName,
            String desk,
            String symbol,
            long limitPriceMinorUnits
    ) {
    }
}

/*
Note: 
Because @SpringBootApplication in Lesson10 auto-configures Netty when started via mvn spring-boot:run, Lesson01–Lesson09 continue running as standalone main() classes via exec:java without starting a web server.

*/