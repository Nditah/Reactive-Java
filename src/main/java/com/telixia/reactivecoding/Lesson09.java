package com.telixia.reactivecoding;

import com.telixia.reactivecoding.model.TraderOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * LESSON 09: Observability (Reactive Context) & Deterministic Testing Pipelines
 *
 * LEARNING OBJECTIVES:
 * 1. Understand why ThreadLocal (and standard SLF4J MDC) breaks in reactive pipelines when
 *    execution jumps across Netty event-loop, parallel, and boundedElastic threads.
 * 2. Propagate immutable metadata (correlationId, deskId, clientTier) across thread boundaries
 *    using Reactor's ContextView and .contextWrite().
 * 3. Understand why Context flows UPWARD (from subscriber to publisher), unlike data signals
 *    which flow DOWNWARD.
 * 4. Design pure, time-injectable reactive services (avoiding hardcoded Schedulers) so
 *    Lesson09Test can verify 24-hour market schedules in milliseconds with StepVerifier.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * - Never use ThreadLocal to pass request/trade trace IDs inside a WebFlux pipeline—one
 *   publishOn() or flatMap() onto another thread pool silently loses or corrupts the ID.
 * - Use Reactor's Context (or Micrometer Context Propagation in Spring Boot 4) and place
 *   .contextWrite() at the BOTTOM of the chain so all upstream operators can read it.
 */
public class Lesson09 {

    public static final String CORRELATION_ID_KEY = "X-Correlation-Id";
    public static final String DESK_ID_KEY = "X-Trading-Desk";

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. REACTIVE CONTEXT: Tracing an Order Across Multiple Thread Pools ===");
        // WHY: Notice how this order hops across three different threads:
        //   main -> parallel-* (risk check) -> boundedElastic-* (audit ledger)
        // Despite crossing thread boundaries twice, Mono.deferContextual() reads the exact
        // correlationId and deskId injected via .contextWrite() at the bottom of the chain!
        CountDownLatch latch = new CountDownLatch(1);
        TraderOrder order = new TraderOrder(1, "Amara", "EQUITIES_ALGO", "HSBA.L", 68550L);
        String traceId = "TRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        executeAuditedOrder(order)
                .contextWrite(Context.of(
                        CORRELATION_ID_KEY, traceId,
                        DESK_ID_KEY, "LONDON_ALGO_DESK_01"
                ))
                .subscribe(
                        confirmation -> System.out.println("  [Final Confirmation] " + confirmation),
                        Throwable::printStackTrace,
                        latch::countDown
                );
        latch.await();

        System.out.println("\n=== 2. CONTEXT SCOPING: Inner Overrides vs. Outer Context ===");
        // WHY: Context is immutable and scoped upward. A contextWrite() placed higher up
        // overrides keys only for operators ABOVE it, leaving downstream stages untouched.
        Mono.deferContextual(ctx -> Mono.just("Upstream reads desk=" + ctx.get(DESK_ID_KEY)))
                .contextWrite(ctx -> ctx.put(DESK_ID_KEY, "OVERRIDDEN_DARK_POOL_DESK"))
                .flatMap(upperMsg -> Mono.deferContextual(ctx ->
                        Mono.just(upperMsg + " | Downstream still reads desk=" + ctx.get(DESK_ID_KEY))))
                .contextWrite(Context.of(DESK_ID_KEY, "PRIMARY_LSE_DESK"))
                .subscribe(msg -> System.out.println("  [Context Scoping] " + msg));

        System.out.println("\nLesson 09 main complete. Now run `mvn test` to execute Lesson09Test!");
        Schedulers.shutdownNow();
    }

    /**
     * Production-grade reactive service method:
     * 1. Reads correlation metadata from Reactor Context across thread hops.
     * 2. Does NOT hardcode Schedulers inside time operators (making it 100% deterministic in tests).
     */
    public static Mono<String> executeAuditedOrder(TraderOrder order) {
        return Mono.deferContextual(ctx -> {
                    String corrId = ctx.getOrDefault(CORRELATION_ID_KEY, "NO_TRACE_ID");
                    String desk = ctx.getOrDefault(DESK_ID_KEY, "UNKNOWN_DESK");
                    System.out.printf("  [Stage 1: Ingress]     corrId=%s | desk=%s | thread=%s%n",
                            corrId, desk, Thread.currentThread().getName());
                    return Mono.just(order);
                })
                .publishOn(Schedulers.parallel())
                .flatMap(ord -> Mono.deferContextual(ctx -> {
                    String corrId = ctx.getOrDefault(CORRELATION_ID_KEY, "NO_TRACE_ID");
                    System.out.printf("  [Stage 2: Risk Engine] corrId=%s | symbol=%s | thread=%s%n",
                            corrId, ord.symbol(), Thread.currentThread().getName());
                    return Mono.just(ord);
                }))
                .publishOn(Schedulers.boundedElastic())
                .map(ord -> "EXECUTED[" + ord.symbol() + " @ " + ord.limitPriceMinorUnits() + "p]");
    }

    /**
     * Simulates an end-of-day regulatory trade reporting schedule that emits heartbeats
     * every 6 hours over a 24-hour trading cycle (4 reports total).
     * Used in Lesson09Test to demonstrate StepVerifier.withVirtualTime().
     */
    public static Flux<String> twentyFourHourRegulatoryBatch() {
        return Flux.interval(Duration.ofHours(6))
                .take(4)
                .map(batchIdx -> "MIFID_II_REPORT_BATCH_#" + (batchIdx + 1));
    }
}
