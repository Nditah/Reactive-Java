package com.telixia.reactivecoding;

import com.telixia.reactivecoding.feed.ReactiveMarketFeed;
import com.telixia.reactivecoding.model.MarketTick;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * LESSON 07: Time, Windowing & Micro-Batching (buffer, window, groupBy, sample)
 *
 * LEARNING OBJECTIVES:
 * 1. Master buffer(maxSize, maxTime): Micro-batch high-frequency events to amortize network/DB
 *    round-trips (e.g., flushing 100 trades OR every 250ms, whichever comes first).
 * 2. Distinguish buffer() (emits List<T> in memory) from window() (emits Flux<T> sub-streams
 *    for incremental streaming aggregation without holding all items in a List).
 * 3. Combine groupBy() + window() to compute per-ticker Volume-Weighted Average Price (VWAP)
 *    in real time across an interleaved market feed.
 * 4. Use sample(Duration) to throttle UI/blotter snapshots to a fixed human-readable refresh rate
 *    while the underlying engine processes every single tick at full speed.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * - Always use hybrid size-AND-time batching (`bufferTimeout(maxSize, maxTime)`) in production:
 *   size-only buffers stall indefinitely during quiet market periods (low liquidity), while
 *   time-only buffers risk memory spikes during 14:30 UK market-open bursts.
 * - Beware of groupBy() with unbounded key cardinality (e.g., millions of unique user/order IDs):
 *   each group stays open in memory until completed. Restrict groupBy() to bounded dimensions
 *   like ticker symbols or trading desks.
 */
public class Lesson07 {

    public record VwapBar(
            String symbol,
            long vwapMinorUnits,
            long totalVolume,
            int tickCount
    ) {
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. BUFFERTIMEOUT: Hybrid Size-and-Time Micro-Batching ===");
        // WHY: Writing individual rows to Postgres/R2DBC at 10,000 trades/sec saturates I/O.
        // bufferTimeout(5, 250ms) flushes a batch as soon as EITHER 5 ticks accumulate OR 250ms elapses,
        // guaranteeing both high throughput under load and bounded tail latency during quiet periods.
        CountDownLatch latch1 = new CountDownLatch(1);
        ReactiveMarketFeed.liveLseTickStream(Duration.ofMillis(40))
                .take(12)
                .bufferTimeout(5, Duration.ofMillis(250))
                .subscribe(
                        batch -> System.out.printf("  [Micro-Batch Flush] Persisted %d ticks in 1 DB round-trip (first=%s @ %dp)%n",
                                batch.size(), batch.getFirst().symbol(), batch.getFirst().priceMinorUnits()),
                        Throwable::printStackTrace,
                        latch1::countDown
                );
        latch1.await();

        System.out.println("\n=== 2. GROUPBY + WINDOW: Real-Time Per-Symbol VWAP Engine ===");
        // WHY: Real exchange feeds interleave ticks for HSBA.L, BARC.L, AZN.L, SHEL.L, ULVR.L on one socket.
        // 1) groupBy(MarketTick::symbol) demultiplexes the single stream into per-ticker sub-streams.
        // 2) window(Duration.ofMillis(300)) slices each ticker stream into 300ms tumbling time buckets.
        // 3) reduce() computes VWAP = Sum(price * volume) / Sum(volume) incrementally without allocating Lists!
        CountDownLatch latch2 = new CountDownLatch(1);
        ReactiveMarketFeed.liveLseTickStream(Duration.ofMillis(25))
                .take(35)
                .groupBy(MarketTick::symbol)
                .flatMap(symbolFlux -> symbolFlux
                        .window(Duration.ofMillis(300))
                        .flatMap(windowFlux -> computeVwap(symbolFlux.key(), windowFlux)))
                .subscribe(
                        bar -> System.out.printf("  [300ms VWAP Bar] %-7s | VWAP=%7dp | vol=%6d | ticks=%d%n",
                                bar.symbol(), bar.vwapMinorUnits(), bar.totalVolume(), bar.tickCount()),
                        Throwable::printStackTrace,
                        latch2::countDown
                );
        latch2.await();

        System.out.println("\n=== 3. SAMPLE: Throttled Trader Blotter Snapshot (Every 200ms) ===");
        // WHY: A matching engine may emit 50,000 ticks/sec, but a human trader's WebSocket/SSE screen
        // cannot render faster than 5-10 FPS. sample(200ms) emits only the most recent tick in each
        // 200ms interval without slowing down the upstream feed.
        CountDownLatch latch3 = new CountDownLatch(1);
        ReactiveMarketFeed.liveLseTickStream(Duration.ofMillis(30))
                .take(25)
                .sample(Duration.ofMillis(200))
                .subscribe(
                        tick -> System.out.printf("  [Blotter UI Snapshot] %-7s @ %7dp (venue=%s)%n",
                                tick.symbol(), tick.priceMinorUnits(), tick.venue()),
                        Throwable::printStackTrace,
                        latch3::countDown
                );
        latch3.await();

        System.out.println("\nLesson 07 complete.");
        Schedulers.shutdownNow();
    }

    /**
     * Incrementally folds a Flux<MarketTick> window into a single VwapBar using O(1) memory.
     * Accumulator array: [0] = sum(price * volume), [1] = sum(volume), [2] = tickCount
     */
    private static reactor.core.publisher.Mono<VwapBar> computeVwap(String symbol, Flux<MarketTick> window) {
        return window
                .reduce(new long[3], (acc, tick) -> {
                    acc[0] += tick.priceMinorUnits() * tick.volume();
                    acc[1] += tick.volume();
                    acc[2] += 1;
                    return acc;
                })
                .filter(acc -> acc[1] > 0) // Ignore empty time windows with zero volume
                .map(acc -> new VwapBar(
                        symbol,
                        acc[0] / acc[1],
                        acc[1],
                        (int) acc[2]
                ));
    }
}


/*
What to observe in the console output:
• In Section 1, 12 ticks flush in deterministic batches of 5, 5, 2 via bufferTimeout.
• In Section 2, interleaved ticks from the live LSE stream are split by ticker symbol and reduced into 300ms VWAP bars using long[3] accumulators with zero intermediate List allocations.
• In Section 3, sample(200ms) downsamples the 30ms feed into clean, human-readable UI frames.

*/