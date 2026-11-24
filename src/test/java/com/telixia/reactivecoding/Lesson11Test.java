package com.telixia.reactivecoding;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LESSON 11 TEST SUITE: Multi-Core ParallelFlux, Bulkhead Bounds & Java 25 Virtual Threads
 */
class Lesson11Test {

    @Test
    void shouldDistributeCpuWorkAcrossMultipleParallelRails() {
        Set<String> workerThreads = ConcurrentHashMap.newKeySet();

        Flux<Integer> parallelPipeline = Flux.range(1, 16)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(id -> {
                    workerThreads.add(Thread.currentThread().getName());
                    return id * 10;
                })
                .sequential();

        StepVerifier.create(parallelPipeline.collectList())
                .assertNext(results -> {
                    assertEquals(16, results.size());
                    assertTrue(workerThreads.size() > 1,
                            "Expected work to execute across multiple parallel worker threads");
                })
                .verifyComplete();
    }

    @Test
    void shouldStrictlyEnforceMaxConcurrencyBulkheadInFlatMap() {
        final int maxConcurrency = 3;
        AtomicInteger active = new AtomicInteger(0);
        AtomicInteger peakObserved = new AtomicInteger(0);

        Flux<Integer> boundedPipeline = Flux.range(1, 25)
                .flatMap(id -> Mono.defer(() -> {
                    int current = active.incrementAndGet();
                    peakObserved.accumulateAndGet(current, Math::max);
                    return Mono.delay(Duration.ofMillis(20))
                            .map(ignored -> {
                                active.decrementAndGet();
                                return id;
                            });
                }), maxConcurrency);

        StepVerifier.create(boundedPipeline.collectList())
                .assertNext(results -> {
                    assertEquals(25, results.size());
                    assertEquals(0, active.get());
                    assertEquals(maxConcurrency, peakObserved.get(),
                            "Peak concurrency must match bulkhead limit " + maxConcurrency);
                })
                .verifyComplete();
    }

    @Test
    void shouldEnforceSemaphoreBulkheadAndScopedValueOnJava25VirtualThreads() throws Exception {
        final ScopedValue<String> traceKey = ScopedValue.newInstance();
        final int maxPermits = 4;
        Semaphore bulkhead = new Semaphore(maxPermits);
        AtomicInteger active = new AtomicInteger(0);
        AtomicInteger peak = new AtomicInteger(0);

        ScopedValue.where(traceKey, "VT-TRACE-2026").run(() -> {
            String boundTrace = traceKey.get();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Callable<Boolean>> tasks = IntStream.range(0, 20)
                        .<Callable<Boolean>>mapToObj(i -> () ->
                                ScopedValue.where(traceKey, boundTrace).call(() -> {
                                    bulkhead.acquire();
                                    try {
                                        int current = active.incrementAndGet();
                                        peak.accumulateAndGet(current, Math::max);
                                        Thread.sleep(15);
                                        return Thread.currentThread().isVirtual()
                                                && "VT-TRACE-2026".equals(traceKey.get());
                                    } finally {
                                        active.decrementAndGet();
                                        bulkhead.release();
                                    }
                                }))
                        .toList();

                List<Future<Boolean>> futures = executor.invokeAll(tasks);
                assertEquals(20, futures.size());
                for (Future<Boolean> f : futures) {
                    assertTrue(f.get(), "Expected virtual thread execution with bound ScopedValue");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(peak.get() <= maxPermits,
                "Virtual thread peak concurrency " + peak.get() + " exceeded semaphore bound " + maxPermits);
    }
}
