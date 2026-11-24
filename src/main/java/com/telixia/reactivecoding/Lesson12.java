package com.telixia.reactivecoding;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * LESSON 12: Lock-Free Order Book State (CAS / AtomicReference / LongAdder)
 *            & Idempotent Trade Execution
 *
 * LEARNING OBJECTIVES:
 * 1. Reproduce a classic "Lost Update" race condition when concurrent reactive flatMap()
 *    workers mutate shared non-atomic state (Java Concurrency in Practice, Chapter 2).
 * 2. Fix shared state contention WITHOUT synchronized locks using Compare-And-Swap (CAS)
 *    via AtomicReference.updateAndGet() over an immutable Java record.
 * 3. Use LongAdder instead of AtomicLong for ultra-high-frequency throughput counters
 *    (striped per-CPU-core cells eliminate cache-line ping-pong under heavy contention).
 * 4. Implement an idempotency store (ConcurrentHashMap.computeIfAbsent) so retried
 *    payment/order commands return the cached receipt instead of double-executing.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * Never use `synchronized` blocks inside reactive pipelines (they serialize worker threads
 * and introduce Amdahl's Law bottlenecks). Instead, model shared state as an immutable record
 * updated atomically via CAS (`AtomicReference.updateAndGet`), use `LongAdder` for hot
 * counters, and protect every retriable financial mutation with an idempotency key.
 */
public class Lesson12 {

    /**
     * Immutable snapshot of an instrument's Best Bid / Best Offer (BBO) and cumulative volume.
     * Because all fields are final, publishing a new instance via AtomicReference is 100% thread-safe.
     */
    public record OrderBookSnapshot(
            String symbol,
            long bestBidPence,
            long bestAskPence,
            long cumulativeVolume
    ) {
        public OrderBookSnapshot applyTrade(long tradePricePence, long tradeQty) {
            long updatedBid = Math.max(bestBidPence, tradePricePence);
            long updatedAsk = Math.min(bestAskPence, tradePricePence + 2);
            return new OrderBookSnapshot(symbol, updatedBid, updatedAsk, cumulativeVolume + tradeQty);
        }
    }

    /**
     * Intentionally non-thread-safe counter to demonstrate lost updates under parallel flatMap().
     */
    static class UnsafePositionTracker {
        long totalShares = 0;

        void recordFill(long qty) {
            long current = totalShares; // Read
            Thread.yield();             // Widen race window across parallel workers
            totalShares = current + qty; // Non-atomic write -> LOST UPDATES!
        }
    }

    public static void main(String[] args) throws InterruptedException {

        final int totalFills = 1_000;
        final long sharesPerFill = 10L;
        final long expectedTotalShares = totalFills * sharesPerFill; // 10,000 shares

        System.out.println("\n=== 1. RACE CONDITION DEMO: Unsafe Shared State vs. Lock-Free CAS & LongAdder ===");
        // WHY: When 1,000 fill events flow through flatMap() across Schedulers.parallel() workers,
        // non-atomic read-modify-write loses updates, whereas AtomicReference (CAS) and LongAdder
        // preserve 100% accuracy without blocking locks!
        UnsafePositionTracker unsafeTracker = new UnsafePositionTracker();
        LongAdder lockFreeVolumeCounter = new LongAdder();
        AtomicReference<OrderBookSnapshot> atomicOrderBook =
                new AtomicReference<>(new OrderBookSnapshot("HSBA.L", 68500L, 68550L, 0L));

        CountDownLatch latch1 = new CountDownLatch(1);
        Flux.range(1, totalFills)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .doOnNext(seq -> {
                    // 1) Unsafe mutation (loses updates)
                    unsafeTracker.recordFill(sharesPerFill);

                    // 2) Lock-free striped counter (Java 8+ LongAdder)
                    lockFreeVolumeCounter.add(sharesPerFill);

                    // 3) Lock-free CAS update on immutable OrderBookSnapshot record
                    long tickPrice = 68500L + (seq % 20);
                    atomicOrderBook.updateAndGet(prev -> prev.applyTrade(tickPrice, sharesPerFill));
                })
                .sequential()
                .subscribe(
                        ignored -> {},
                        Throwable::printStackTrace,
                        () -> {
                            OrderBookSnapshot finalBook = atomicOrderBook.get();
                            System.out.println("  [Expected Volume]       " + expectedTotalShares + " shares");
                            System.out.println("  [Unsafe Tracker]        " + unsafeTracker.totalShares + " shares (LOST UPDATES!)");
                            System.out.println("  [LongAdder (Lock-Free)] " + lockFreeVolumeCounter.sum() + " shares (100% exact)");
                            System.out.println("  [AtomicReference CAS]   " + finalBook + " (100% exact)");
                            latch1.countDown();
                        }
                );
        latch1.await();

        System.out.println("\n=== 2. IDEMPOTENT ORDER EXECUTION: Preventing Double-Fills on Network Retry ===");
        // WHY: In Lesson 08 we retried transient network timeouts with Retry.backoff().
        // If a client or gateway retries `CLIENT-ORD-7788` because the HTTP ACK timed out,
        // the ledger MUST NOT execute the trade twice!
        // ConcurrentHashMap.computeIfAbsent() ensures the Mono is created and cached atomically
        // per idempotencyKey, and .cache() replays the exact same result to duplicate callers.
        ConcurrentHashMap<String, Mono<String>> idempotencyStore = new ConcurrentHashMap<>();
        LongAdder actualLedgerExecutions = new LongAdder();

        CountDownLatch latch2 = new CountDownLatch(1);
        Flux.just(
                        "CLIENT-ORD-7788", // Initial request
                        "CLIENT-ORD-7788", // Duplicate retry #1
                        "CLIENT-ORD-7788", // Duplicate retry #2
                        "CLIENT-ORD-9901"  // Distinct new order
                )
                .flatMap(idempotencyKey -> submitOrderIdempotently(
                        idempotencyKey, idempotencyStore, actualLedgerExecutions))
                .subscribe(
                        receipt -> System.out.println("  [Client Response] " + receipt),
                        Throwable::printStackTrace,
                        () -> {
                            System.out.println("  -> Total requests received: 4 | Actual ledger executions: "
                                    + actualLedgerExecutions.sum() + " (Zero double-charges!)");
                            latch2.countDown();
                        }
                );
        latch2.await();

        System.out.println("\nLesson 12 complete. Congratulations—you finished the entire 12-lesson workshop!");
        Schedulers.shutdownNow();
    }

    /**
     * Guarantees at-most-once execution per idempotencyKey under concurrent retries.
     */
    private static Mono<String> submitOrderIdempotently(
            String idempotencyKey,
            ConcurrentHashMap<String, Mono<String>> store,
            LongAdder ledgerExecutionCounter
    ) {
        return store.computeIfAbsent(idempotencyKey, key ->
                Mono.delay(Duration.ofMillis(60))
                        .map(ignored -> {
                            ledgerExecutionCounter.increment();
                            long execSeq = ledgerExecutionCounter.sum();
                            return "SETTLED[key=" + key + ", ledgerTxId=TX-" + execSeq + "]";
                        })
                        .cache() // Cache the terminal result so concurrent/subsequent retries get the same receipt
        );
    }
}
