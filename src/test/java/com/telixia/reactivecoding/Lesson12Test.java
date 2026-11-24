package com.telixia.reactivecoding;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LESSON 12 TEST SUITE: Concurrency Correctness, Lock-Free CAS & Idempotency Tests
 *
 * Follows Java Concurrency in Practice (Chapter 12) testing principles:
 * - Uses CountDownLatch starting gates to maximize thread contention on shared state.
 * - Verifies multivariable invariants (BBO spread and cumulative volume) under parallel load.
 * - Verifies at-most-once execution for duplicate retried order keys.
 */
class Lesson12Test {

    @Test
    void shouldPreserveOrderBookInvariantsWithoutLostUpdatesUnderHeavyContention() throws InterruptedException {
        final int threads = 8;
        final int updatesPerThread = 2_500;
        final long qtyPerTrade = 5L;
        final long expectedVolume = (long) threads * updatesPerThread * qtyPerTrade; // 100,000

        AtomicReference<Lesson12.OrderBookSnapshot> bookRef =
                new AtomicReference<>(new Lesson12.OrderBookSnapshot("HSBA.L", 68500L, 68600L, 0L));
        LongAdder volumeAdder = new LongAdder();

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final long basePrice = 68510L + t;
            Thread.ofPlatform().start(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < updatesPerThread; i++) {
                        long price = basePrice + (i % 15);
                        bookRef.updateAndGet(prev -> prev.applyTrade(price, qtyPerTrade));
                        volumeAdder.add(qtyPerTrade);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endGate.countDown();
                }
            });
        }

        // Release all 8 threads simultaneously to maximize CAS retry contention
        startGate.countDown();
        endGate.await();

        Lesson12.OrderBookSnapshot finalSnapshot = bookRef.get();
        assertEquals(expectedVolume, volumeAdder.sum(), "LongAdder volume must equal exact expected total");
        assertEquals(expectedVolume, finalSnapshot.cumulativeVolume(), "CAS snapshot volume must have zero lost updates");
        assertTrue(finalSnapshot.bestBidPence() >= 68500L);
        assertTrue(finalSnapshot.bestAskPence() <= 68600L);
    }

    @Test
    void shouldExecuteAtMostOncePerIdempotencyKeyUnderConcurrentRetries() {
        ConcurrentHashMap<String, Mono<String>> idempotencyStore = new ConcurrentHashMap<>();
        LongAdder executionCount = new LongAdder();

        // 50 concurrent duplicate submissions for "ORD-DUP-001" and 50 for "ORD-DUP-002"
        Flux<String> concurrentRetries = Flux.range(0, 100)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .flatMap(i -> {
                    String key = (i % 2 == 0) ? "ORD-DUP-001" : "ORD-DUP-002";
                    return idempotencyStore.computeIfAbsent(key, k ->
                            Mono.delay(Duration.ofMillis(25))
                                    .map(ignored -> {
                                        executionCount.increment();
                                        return "RECEIPT-" + k;
                                    })
                                    .cache()
                    );
                })
                .sequential();

        StepVerifier.create(concurrentRetries.collectList())
                .assertNext((List<String> receipts) -> {
                    assertEquals(100, receipts.size(), "All 100 callers must receive a valid receipt");
                    assertEquals(2L, executionCount.sum(),
                            "Underlying ledger execution must run exactly once per unique idempotency key");
                    long dup1Count = receipts.stream().filter("RECEIPT-ORD-DUP-001"::equals).count();
                    long dup2Count = receipts.stream().filter("RECEIPT-ORD-DUP-002"::equals).count();
                    assertEquals(50L, dup1Count);
                    assertEquals(50L, dup2Count);
                })
                .verifyComplete();
    }
}
