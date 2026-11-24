package com.telixia.reactivecoding;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IN-JVM ENGINE STRESS BENCHMARK:
 * Measures sustained TPS, extrapolated TPM, and p50/p95/p99 tail latencies
 * on the multi-core lock-free matching pipeline after JVM C2 JIT warmup.
 *
 * Run via: npm run stress:engine
 */
@Tag("stress")
class OrderEngineStressTest {

    @Test
    void measureInJvmOrderBookThroughputAndTailLatency() {
        final int warmupTransactions = 100_000;
        final int benchmarkTransactions = 1_000_000;
        final int rails = Runtime.getRuntime().availableProcessors();

        // 1. JVM JIT Warmup (eliminates cold-start C1/C2 compilation skew)
        runBatch(warmupTransactions, rails, null);

        // 2. Measured Run: 1,000,000 lock-free CAS order book updates
        long[] latenciesNs = new long[benchmarkTransactions];
        long startNs = System.nanoTime();

        Lesson12.OrderBookSnapshot finalBook = runBatch(benchmarkTransactions, rails, latenciesNs);

        long totalDurationNs = System.nanoTime() - startNs;
        double durationSec = totalDurationNs / 1_000_000_000.0;
        double tps = benchmarkTransactions / durationSec;
        double tpm = tps * 60.0;

        Arrays.sort(latenciesNs);
        double p50Us = latenciesNs[(int) (benchmarkTransactions * 0.50)] / 1_000.0;
        double p95Us = latenciesNs[(int) (benchmarkTransactions * 0.95)] / 1_000.0;
        double p99Us = latenciesNs[(int) (benchmarkTransactions * 0.99)] / 1_000.0;

        System.out.printf("""
                
                ================ IN-JVM ENGINE STRESS REPORT ================
                CPU Rails (Cores) : %d
                Total Transactions: %,d
                Elapsed Time      : %.3f s
                Throughput (TPS)  : %,.0f tx/sec
                Throughput (TPM)  : %,.0f tx/min
                Latency p50       : %.2f µs
                Latency p95       : %.2f µs
                Latency p99       : %.2f µs
                =============================================================
                """, rails, benchmarkTransactions, durationSec, tps, tpm, p50Us, p95Us, p99Us);

        assertEquals(benchmarkTransactions * 10L, finalBook.cumulativeVolume());
        assertTrue(tps > 50_000, "Expected > 50,000 TPS on multi-core lock-free pipeline");
    }

    private Lesson12.OrderBookSnapshot runBatch(int count, int rails, long[] latenciesNs) {
        AtomicReference<Lesson12.OrderBookSnapshot> book =
                new AtomicReference<>(new Lesson12.OrderBookSnapshot("HSBA.L", 68500L, 68600L, 0L));
        LongAdder counter = new LongAdder();

        Flux.range(0, count)
                .parallel(rails)
                .runOn(Schedulers.parallel())
                .doOnNext(i -> {
                    long t0 = (latenciesNs != null) ? System.nanoTime() : 0L;
                    long price = 68510L + (i & 31);
                    book.updateAndGet(prev -> prev.applyTrade(price, 10L));
                    counter.increment();
                    if (latenciesNs != null) {
                        latenciesNs[i] = System.nanoTime() - t0;
                    }
                })
                .sequential()
                .blockLast();

        return book.get();
    }
}
