package com.telixia.reactivecoding;

import com.telixia.reactivecoding.feed.ReactiveMarketFeed;
import com.telixia.reactivecoding.model.TraderOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;

/**
 * LESSON 06: Concurrency, Schedulers & Event-Loop Hygiene (publishOn vs. subscribeOn)
 *
 * LEARNING OBJECTIVES:
 * 1. Understand that Project Reactor is concurrency-agnostic by default: operators execute
 *    on whatever thread invokes .subscribe() or emits the upstream signal.
 * 2. Master the exact difference between subscribeOn() and publishOn():
 *    - subscribeOn(scheduler): Controls where the SOURCE subscription begins (propagates UPWARD).
 *      Position in the chain does NOT matter, and only the FIRST subscribeOn wins.
 *    - publishOn(scheduler):   Switches the execution thread for all DOWNSTREAM operators
 *      from that point onward (propagates DOWNWARD). Can be used multiple times.
 * 3. Isolate legacy blocking I/O (e.g., blocking JDBC ledger writes or SOAP/SWIFT gateways)
 *    onto Schedulers.boundedElastic() so Netty event-loop threads are NEVER blocked.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * In Spring WebFlux, Netty runs a tiny event-loop pool (~1 thread per CPU core) that
 * multiplexes tens of thousands of concurrent connections. Calling Thread.sleep(), JDBC,
 * or blocking HTTP inside an event-loop thread is the cardinal sin of reactive Java—it
 * freezes every connection assigned to that event loop. Wrap legacy blocking calls in
 * Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic()) and verify in CI with BlockHound.
 */

public class Lesson06 {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. DEFAULT EXECUTION: Runs on the caller's thread ===");
        Flux.just("HSBA.L", "BARC.L")
                .map(symbol -> {
                    logThread("map(" + symbol + ")");
                    return symbol.toLowerCase();
                })
                .subscribe(res -> logThread("subscribe(" + res + ")"));

        System.out.println("\n=== 2. SUBSCRIBEON: Moves source execution upward (first one wins) ===");
        CountDownLatch latch2 = new CountDownLatch(1);
        Mono.fromCallable(() -> {
                    logThread("Callable source (reads legacy settlement config)");
                    return "SETTLEMENT_BATCH_2026_11_24";
                })
                .map(batch -> {
                    logThread("map(validate " + batch + ")");
                    return batch;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribeOn(Schedulers.parallel())
                .subscribe(
                        batch -> logThread("subscribe(" + batch + ")"),
                        Throwable::printStackTrace,
                        latch2::countDown
                );
        latch2.await();

        System.out.println("\n=== 3. PUBLISHON: Switches threads for downstream stages ===");
        CountDownLatch latch3 = new CountDownLatch(1);
        Flux.range(1, 3)
                .doOnNext(tick -> logThread("1. Ingest tick #" + tick))
                .publishOn(Schedulers.parallel())
                .map(tick -> {
                    logThread("2. CPU Monte-Carlo pricing for tick #" + tick);
                    return "PricedTick-#" + tick;
                })
                .publishOn(Schedulers.single())
                .subscribe(
                        priced -> logThread("3. Sequential audit log write: " + priced),
                        Throwable::printStackTrace,
                        latch3::countDown
                );
        latch3.await();

        System.out.println("\n=== 4. ISOLATING BLOCKING LEGACY JDBC CALLS (boundedElastic) ===");
        CountDownLatch latch4 = new CountDownLatch(1);
        long start = System.nanoTime();

        ReactiveMarketFeed.orderBookFlux()
                .take(4)
                .doOnNext(order -> logThread("Event-Loop received order: " + order.symbol()))
                .flatMap(order -> persistToLegacyBlockingLedger(order)
                        .subscribeOn(Schedulers.boundedElastic()))
                .subscribe(
                        receipt -> logThread("Confirmed receipt: " + receipt),
                        Throwable::printStackTrace,
                        () -> {
                            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                            System.out.println("  -> All 4 blocking ledger writes isolated & completed in "
                                    + elapsedMs + "ms without blocking event-loop threads!");
                            latch4.countDown();
                        }
                );
        latch4.await();

        System.out.println("\nLesson 06 complete.");
        Schedulers.shutdownNow();
    }

    private static Mono<String> persistToLegacyBlockingLedger(TraderOrder order) {
        return Mono.fromCallable(() -> {
            logThread("  [Blocking JDBC] Writing ledger entry for " + order.symbol());
            Thread.sleep(200);
            return "LEDGER_TX_" + order.instrumentId() + "_" + order.traderName();
        });
    }

    private static void logThread(String stage) {
        System.out.printf("  %-52s | thread=%s%n", stage, Thread.currentThread().getName());
    }
}


/*
What to observe in the console output:
• In Section 2, subscribeOn(Schedulers.boundedElastic()) wins over the second subscribeOn(Schedulers.parallel()) and moves the upstream Callable off main.
• In Section 3, publishOn() cleanly shifts execution from main -> parallel-* -> single-* as the data flows downward.
• In Section 4, notice that placing .subscribeOn(Schedulers.boundedElastic()) inside flatMap() lets all four 200ms blocking JDBC calls run concurrently on boundedElastic-* threads rather than stalling the upstream parallel-* event threads.
*/