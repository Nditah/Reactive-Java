package com.telixia.reactivecoding;

import com.telixia.reactivecoding.model.MarketTick;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LESSON 05: Backpressure & Overload Protection during a 14:30 UK Market-Open Burst
 *
 * LEARNING OBJECTIVES:
 * 1. Understand Reactive Streams pull-push demand signalling via Subscription.request(n).
 * 2. Use limitRate(n) to enforce bounded prefetching between fast producers and slow consumers.
 * 3. Compare the four overload strategies when a hot producer ignores downstream demand
 *    (e.g., UDP market-data multicast or bursty WebSocket feed):
 *    - onBackpressureDrop:   Shed excess ticks immediately and log/meter dropped volume.
 *    - onBackpressureLatest: Conflate stale ticks—keep ONLY the freshest quote in flight.
 *    - onBackpressureBuffer: Absorb brief micro-bursts in a bounded ring buffer before failing/dropping.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * Unbounded queues are an OutOfMemoryError waiting to happen under sustained load.
 * In electronic trading, when a downstream risk/pricing engine falls behind a bursty
 * market-data feed, buffering stale prices is worse than dropping them—trading on a
 * 500ms-old quote causes adverse selection. Use onBackpressureLatest() for market prices
 * (conflation) and bounded buffers with explicit overflow policies for order/ledger events.
 */
public class Lesson05 {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. EXPLICIT DEMAND SIGNALLING: BaseSubscriber & request(n) ===");
        // WHY: In Reactive Streams, a well-behaved cold publisher NEVER pushes data faster
        // than the subscriber asks for via subscription.request(n).
        CountDownLatch latch1 = new CountDownLatch(1);
        Flux.range(1, 6)
                .map(seq -> createTick(seq, "HSBA.L", 68500L + seq))
                .doOnRequest(n -> System.out.println("  [Upstream] Downstream requested batch of: " + n))
                .subscribe(new BaseSubscriber<>() {
                    private int processedInBatch = 0;
                    private static final int BATCH_SIZE = 2;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        System.out.println("  [Subscriber] Connected. Requesting initial batch of " + BATCH_SIZE);
                        request(BATCH_SIZE);
                    }

                    @Override
                    protected void hookOnNext(MarketTick tick) {
                        System.out.println("  [Subscriber] Processed tick #" + tick.instrumentId()
                                + " @ " + tick.priceMinorUnits() + "p");
                        processedInBatch++;
                        if (processedInBatch == BATCH_SIZE) {
                            processedInBatch = 0;
                            request(BATCH_SIZE);
                        }
                    }

                    @Override
                    protected void hookOnComplete() {
                        System.out.println("  [Subscriber] Batch processing complete.");
                        latch1.countDown();
                    }
                });
        latch1.await();

        System.out.println("\n=== 2. LIMITRATE: Bounded Prefetching for Cold Producers ===");
        // WHY: By default, operators like publishOn() prefetch 256 items upfront.
        // limitRate(3) caps in-flight demand to 3 items and automatically replenishes at 75% (2 items),
        // keeping memory footprint strictly bounded.
        CountDownLatch latch2 = new CountDownLatch(1);
        Flux.range(1, 8)
                .doOnRequest(n -> System.out.println("  [limitRate Upstream] Demand received: " + n))
                .limitRate(3)
                .publishOn(Schedulers.parallel())
                .subscribe(
                        item -> System.out.println("  [Consumer] Handled item #" + item),
                        Throwable::printStackTrace,
                        latch2::countDown
                );
        latch2.await();

        System.out.println("\n=== 3. ONBACKPRESSUREDROP: Load Shedding During 14:30 UK Market-Open Spike ===");
        // WHY: Flux.interval() is a HOT time-driven producer—it cannot slow down the exchange clock!
        // Here, ticks fire every 10ms (100 ticks/sec), while our slow risk calculator takes 60ms per tick.
        // Without a backpressure strategy, Reactor throws Exceptions$OverflowException ("The receiver is overrun").
        // onBackpressureDrop() cleanly discards ticks arriving while the consumer is busy.
        CountDownLatch latch3 = new CountDownLatch(1);
        AtomicInteger droppedCount = new AtomicInteger();

        Flux.interval(Duration.ofMillis(10))
                .take(25)
                .map(seq -> createTick(seq.intValue(), "BARC.L", 21400L + seq))
                .onBackpressureDrop(dropped -> droppedCount.incrementAndGet())
                .publishOn(Schedulers.parallel(), 1) // Consumer queue size = 1 to expose overload immediately
                .concatMap(tick -> slowRiskCalculation(tick, 60))
                .subscribe(
                        result -> System.out.println("  [Drop Strategy] " + result),
                        Throwable::printStackTrace,
                        () -> {
                            System.out.println("  -> Total ticks shed under load: " + droppedCount.get());
                            latch3.countDown();
                        }
                );
        latch3.await();

        System.out.println("\n=== 4. ONBACKPRESSURELATEST: Quote Conflation (Always Trade on Fresh Prices) ===");
        // WHY: For live market prices, dropping newer ticks in favour of older buffered ones is dangerous.
        // onBackpressureLatest() overwrites any unconsumed tick with the single LATEST price,
        // guaranteeing the risk/pricing engine always sees the freshest market state once free.
        CountDownLatch latch4 = new CountDownLatch(1);
        Flux.interval(Duration.ofMillis(10))
                .take(25)
                .map(seq -> createTick(seq.intValue(), "AZN.L", 1184000L + (seq * 10)))
                .onBackpressureLatest()
                .publishOn(Schedulers.parallel(), 1)
                .concatMap(tick -> slowRiskCalculation(tick, 60))
                .subscribe(
                        result -> System.out.println("  [Latest Strategy] " + result),
                        Throwable::printStackTrace,
                        latch4::countDown
                );
        latch4.await();

        System.out.println("\n=== 5. ONBACKPRESSUREBUFFER: Bounded Shock Absorber ===");
        // WHY: For short micro-bursts where losing data is unacceptable, a bounded buffer absorbs
        // spikes up to maxSize (here 5 items). If the burst persists, BufferOverflowStrategy.DROP_OLDEST
        // evicts stale entries rather than blowing up JVM heap memory.
        CountDownLatch latch5 = new CountDownLatch(1);
        Flux.interval(Duration.ofMillis(10))
                .take(15)
                .map(seq -> createTick(seq.intValue(), "SHEL.L", 279000L + seq))
                .onBackpressureBuffer(
                        5,
                        dropped -> System.out.println("    [Buffer Full] Evicted oldest tick #" + dropped.instrumentId()),
                        BufferOverflowStrategy.DROP_OLDEST
                )
                .publishOn(Schedulers.parallel(), 1)
                .concatMap(tick -> slowRiskCalculation(tick, 40))
                .subscribe(
                        result -> System.out.println("  [Bounded Buffer] " + result),
                        Throwable::printStackTrace,
                        latch5::countDown
                );
        latch5.await();

        System.out.println("\nLesson 05 complete.");
        Schedulers.shutdownNow();
    }

    private static MarketTick createTick(int seq, String symbol, long price) {
        return new MarketTick(seq, symbol, price, 500L, "LSE", Instant.now());
    }

    private static reactor.core.publisher.Mono<String> slowRiskCalculation(MarketTick tick, long processingMs) {
        return reactor.core.publisher.Mono.delay(Duration.ofMillis(processingMs))
                .map(ignored -> String.format("Evaluated %s tick #%02d @ %dp",
                        tick.symbol(), tick.instrumentId(), tick.priceMinorUnits()));
    }
}

/*
What to observe in the console output:
• In Section 1, the producer only emits 2 items at a time after the subscriber explicitly calls request(2).
• In Section 2, limitRate(3) requests 3 items initially and replenishes in batches of 2 (75% threshold) automatically.
• In Sections 3 & 4, notice the jump in tick sequence numbers (#00, #05, #11, #17, #24): onBackpressureLatest() always jumps straight to the newest quote (#24 at the end) rather than falling behind on stale ticks.
*/