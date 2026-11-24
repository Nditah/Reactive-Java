package com.telixia.reactivecoding;

import com.telixia.reactivecoding.feed.ReactiveMarketFeed;
import com.telixia.reactivecoding.model.TraderOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LESSON 03: Non-Blocking Transformations & Async Joins
 *            (map vs. flatMap vs. concatMap vs. switchMap)
 *
 * LEARNING OBJECTIVES:
 * 1. Understand why calling a non-blocking I/O method inside map() produces a nested
 *    Flux<Mono<T>> (the exact equivalent of Array.map(async fn) -> Promise[] in JS).
 * 2. Master the three async flattening strategies and their latency/ordering trade-offs:
 *    - flatMap:   Eager parallel subscription; highest throughput, NO order guarantee.
 *    - concatMap: Sequential 1-by-1 subscription; preserves strict causal order, higher latency.
 *    - switchMap: Cancels previous in-flight work when a newer event arrives; ideal for stale quotes.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * In trading and banking systems, choosing between flatMap and concatMap is a business
 * correctness decision, not just a performance tweak:
 * - Use concatMap for per-account ledger mutations where event order determines balance validity.
 * - Use flatMap (with bounded concurrency) for independent read-only risk/enrichment lookups.
 * - Use switchMap for UI/pricing engines where a newer market tick invalidates an older quote calculation.
 */
public class Lesson03 {

    /**
     * Immutable result of an asynchronous pre-trade credit & margin check.
     */
    public record MarginCheckResult(
            TraderOrder order,
            boolean approved,
            long latencyMs,
            String threadName
    ) {
    }

    public static void main(String[] args) throws InterruptedException {

        // Take 4 incoming orders from our non-blocking order book feed
        Flux<TraderOrder> incomingOrders = ReactiveMarketFeed.orderBookFlux().take(4);

        System.out.println("\n=== 1. MAP: Synchronous 1-to-1 in-memory transformation ===");
        // WHY: Use map() ONLY when the transformation is pure, CPU-bound, and returns a plain value T
        // (not a Mono<T> or Flux<T>). If you call an async repo inside map(), you get Flux<Mono<T>>!
        CountDownLatch latch1 = new CountDownLatch(1);
        incomingOrders
                .map(order -> order.symbol() + " (" + order.desk() + ") -> " + order.limitPriceMinorUnits() + "p")
                .subscribe(
                        summary -> System.out.println("  [map] " + summary),
                        Throwable::printStackTrace,
                        latch1::countDown
                );
        latch1.await();

        System.out.println("\n=== 2. FLATMAP: Parallel async enrichment (Max throughput, unordered) ===");
        // WHY: flatMap subscribes to inner Monos eagerly in parallel (default concurrency = 256).
        // Notice how faster checks finish BEFORE slower ones—output order differs from input order!
        CountDownLatch latch2 = new CountDownLatch(1);
        long flatStart = System.nanoTime();
        incomingOrders
                .flatMap(Lesson03::checkPreTradeMarginAsync)
                .subscribe(
                        res -> System.out.printf("  [flatMap]   %-8s | trader=%-6s | latency=%3dms | thread=%s%n",
                                res.order().symbol(), res.order().traderName(), res.latencyMs(), res.threadName()),
                        Throwable::printStackTrace,
                        () -> {
                            long elapsedMs = (System.nanoTime() - flatStart) / 1_000_000;
                            System.out.println("  -> flatMap total wall-clock time: " + elapsedMs + "ms (parallel)");
                            latch2.countDown();
                        }
                );
        latch2.await();

        System.out.println("\n=== 3. CONCATMAP: Sequential async enrichment (Strict order, higher latency) ===");
        // WHY: concatMap waits for inner Mono #1 to emit onComplete before subscribing to inner Mono #2.
        // Total latency is the SUM of all calls, but input order (HSBA -> BARC -> BARC -> AZN) is 100% preserved.
        CountDownLatch latch3 = new CountDownLatch(1);
        long concatStart = System.nanoTime();
        incomingOrders
                .concatMap(Lesson03::checkPreTradeMarginAsync)
                .subscribe(
                        res -> System.out.printf("  [concatMap] %-8s | trader=%-6s | latency=%3dms | thread=%s%n",
                                res.order().symbol(), res.order().traderName(), res.latencyMs(), res.threadName()),
                        Throwable::printStackTrace,
                        () -> {
                            long elapsedMs = (System.nanoTime() - concatStart) / 1_000_000;
                            System.out.println("  -> concatMap total wall-clock time: " + elapsedMs + "ms (sequential)");
                            latch3.countDown();
                        }
                );
        latch3.await();

        System.out.println("\n=== 4. SWITCHMAP: Cancel stale in-flight pricing when a newer tick arrives ===");
        // WHY: Suppose an algo re-prices an option every time a new tick arrives (every 100ms),
        // and each pricing model takes 250ms. Computing quotes for superseded ticks wastes CPU and
        // risks trading on stale prices! switchMap cancels the previous in-flight Mono immediately.
        CountDownLatch latch4 = new CountDownLatch(1);
        Flux.interval(Duration.ofMillis(100))
                .take(5) // Ticks 0, 1, 2, 3, 4 arrive 100ms apart
                .doOnNext(tick -> System.out.println("  [Tick Arrived] #" + tick))
                .switchMap(tick -> calculateOptionQuoteAsync(tick)
                        .doOnCancel(() -> System.out.println("    [Cancelled] Stale calculation for tick #" + tick)))
                .subscribe(
                        quote -> System.out.println("  [switchMap Published] " + quote),
                        Throwable::printStackTrace,
                        latch4::countDown
                );
        latch4.await();

        System.out.println("\nLesson 03 complete.");
        // Cleanly shut down Reactor's daemon worker pools so Maven exec:java exits immediately
        Schedulers.shutdownNow();
    }

    /**
     * Simulates an asynchronous risk/margin microservice call with variable network latency (50ms - 350ms).
     */
    private static Mono<MarginCheckResult> checkPreTradeMarginAsync(TraderOrder order) {
        long simulatedLatency = ThreadLocalRandom.current().nextLong(50, 350);
        return Mono.delay(Duration.ofMillis(simulatedLatency))
                .map(ignored -> new MarginCheckResult(
                        order,
                        order.limitPriceMinorUnits() < 1_500_000L,
                        simulatedLatency,
                        Thread.currentThread().getName()
                ));
    }

    /**
     * Simulates an expensive 250ms quantitative pricing calculation triggered by each market tick.
     */
    private static Mono<String> calculateOptionQuoteAsync(long tickSeq) {
        return Mono.delay(Duration.ofMillis(250))
                .map(ignored -> "FairValueQuote[fromTick=#" + tickSeq + ", spread=1.5bps]");
    }
}

/*
What to observe in the console output:
• In flatMap, the 4 orders finish out of order based on random network jitter, and total wall-clock time is roughly equal to the single slowest call (~350ms).
• In concatMap, the 4 orders finish in exact submission order (HSBA.L, BARC.L, BARC.L, AZN.L), and total wall-clock time is the sum of all 4 calls.
• In switchMap, ticks #0 through #3 are cancelled mid-flight as newer ticks arrive, and only #4 (the latest non-stale tick) completes its 250ms quote calculation.
*/