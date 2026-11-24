package com.telixia.reactivecoding;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * LESSON 11: Multi-Core CPU Parallelism (ParallelFlux), Concurrency Bulkheads
 *            & Java 25 Virtual Threads Side-by-Side
 *
 * LEARNING OBJECTIVES:
 * 1. Parallelize CPU-intensive quant pricing across multiple CPU cores using Reactor's
 *    ParallelFlux (.parallel(rails).runOn(Schedulers.parallel()).sequential()).
 * 2. Understand why .parallel() alone does NOT spawn threads—it only partitions the stream
 *    into round-robin "rails" until .runOn(Schedulers.parallel()) binds each rail to a worker.
 * 3. Enforce concurrency bulkheads via flatMap(fn, maxConcurrency) so an incoming order burst
 *    never overwhelms a downstream risk engine or database connection pool.
 * 4. Compare Project Reactor side-by-side with Java 25 Virtual Threads + Semaphore bulkheads +
 *    ScopedValue (JEP 506, finalized in JDK 25) on the exact same I/O-bound fan-out workload.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * - Reactor's flatMap() multiplexes concurrent I/O on a small fixed thread pool, whereas
 *   ParallelFlux + runOn(Schedulers.parallel()) unlocks true multi-core CPU parallelism.
 * - In Java 25, Virtual Threads eliminate OS thread scarcity for blocking I/O and pair with
 *   ScopedValue (JEP 506) instead of ThreadLocal (re-bound explicitly on unstructured executors,
 *   or inherited automatically via StructuredTaskScope), while Semaphore acts as the concurrency
 *   bulkhead that Reactor provides natively via flatMap(..., maxConcurrency).
 */


public class Lesson11 {

    // JEP 506 (Final in Java 25): Immutable, bounded-lifetime context binding
    private static final ScopedValue<String> DESK_TRACE_ID = ScopedValue.newInstance();

    public static void main(String[] args) throws Exception {

        System.out.println("\n=== 1. PARALLELFLUX: Multi-Core CPU Rail Partitioning ===");
        CountDownLatch latch1 = new CountDownLatch(1);
        long cpuStart = System.nanoTime();

        Flux.range(1, 8)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(optionId -> {
                    double impliedVol = runCpuHeavyPricingMath(optionId);
                    return String.format("Option-#%02d | IV=%.4f | worker=%s",
                            optionId, impliedVol, Thread.currentThread().getName());
                })
                .sequential()
                .subscribe(
                        res -> System.out.println("  [ParallelFlux] " + res),
                        Throwable::printStackTrace,
                        () -> {
                            long elapsedMs = (System.nanoTime() - cpuStart) / 1_000_000;
                            System.out.println("  -> 8 CPU-heavy option pricings finished across 4 rails in " + elapsedMs + "ms");
                            latch1.countDown();
                        }
                );
        latch1.await();

        System.out.println("\n=== 2. CONCURRENCY BULKHEAD: flatMap(fn, maxConcurrency) ===");
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicInteger activeInFlight = new AtomicInteger(0);
        AtomicInteger peakObservedConcurrency = new AtomicInteger(0);

        Flux.range(1, 10)
                .flatMap(orderId -> simulatedDbQueryWithBulkheadProbe(orderId, activeInFlight, peakObservedConcurrency), 3)
                .subscribe(
                        receipt -> System.out.println("  [Bulkhead Receipt] " + receipt),
                        Throwable::printStackTrace,
                        () -> {
                            System.out.println("  -> Peak concurrent DB calls observed: "
                                    + peakObservedConcurrency.get() + " (strictly capped at maxConcurrency=3)");
                            latch2.countDown();
                        }
                );
        latch2.await();

        System.out.println("\n=== 3. JAVA 25 VIRTUAL THREADS + SEMAPHORE + SCOPEDVALUE COMPARISON ===");
        // WHY explicit re-binding on ExecutorService:
        // Per JEP 506, ScopedValue is inherited automatically ONLY by StructuredTaskScope forks
        // (still a preview API in Java 25). With standard Executors.newVirtualThreadPerTaskExecutor(),
        // capture the parent ScopedValue and re-bind it inside each virtual thread's dynamic scope.
        long vtStart = System.nanoTime();
        Semaphore bulkheadSemaphore = new Semaphore(3);
        AtomicInteger vtActive = new AtomicInteger(0);
        AtomicInteger vtPeak = new AtomicInteger(0);

        ScopedValue.where(DESK_TRACE_ID, "VT-DESK-LONDON-25").run(() -> {
            String inheritedTraceId = DESK_TRACE_ID.get();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Callable<String>> tasks = IntStream.rangeClosed(1, 10)
                        .<Callable<String>>mapToObj(orderId -> () ->
                                ScopedValue.where(DESK_TRACE_ID, inheritedTraceId).call(() -> {
                                    bulkheadSemaphore.acquire();
                                    try {
                                        int current = vtActive.incrementAndGet();
                                        vtPeak.accumulateAndGet(current, Math::max);
                                        Thread.sleep(80); // Unmounts cleanly from carrier OS thread
                                        return String.format("Order-#%02d | trace=%s | virtual=%s",
                                                orderId, DESK_TRACE_ID.get(), Thread.currentThread().isVirtual());
                                    } finally {
                                        vtActive.decrementAndGet();
                                        bulkheadSemaphore.release();
                                    }
                                }))
                        .toList();

                List<Future<String>> futures = executor.invokeAll(tasks);
                for (Future<String> future : futures) {
                    System.out.println("  [Java 25 VT] " + future.get());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        long vtElapsedMs = (System.nanoTime() - vtStart) / 1_000_000;
        System.out.println("  -> Virtual Threads completed 10 tasks (peak concurrency="
                + vtPeak.get() + ") in " + vtElapsedMs + "ms");

        System.out.println("\nLesson 11 complete.");
        Schedulers.shutdownNow();
    }

    private static double runCpuHeavyPricingMath(int seed) {
        double acc = seed * 0.015;
        for (int i = 1; i <= 2_500_000; i++) {
            acc += Math.sin(i * 0.0001) * Math.cos(seed);
        }
        return Math.abs(acc % 1.0);
    }

    private static Mono<String> simulatedDbQueryWithBulkheadProbe(
            int orderId,
            AtomicInteger active,
            AtomicInteger peak
    ) {
        return Mono.defer(() -> {
            int current = active.incrementAndGet();
            peak.accumulateAndGet(current, Math::max);
            return Mono.delay(Duration.ofMillis(80))
                    .map(ignored -> {
                        int remaining = active.decrementAndGet();
                        return String.format("Order-#%02d committed (remainingInFlight=%d)", orderId, remaining);
                    });
        });
    }
}
