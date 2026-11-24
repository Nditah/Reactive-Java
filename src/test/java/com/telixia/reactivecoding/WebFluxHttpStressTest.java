package com.telixia.reactivecoding;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * END-TO-END HTTP STRESS BENCHMARK:
 * Boots Lesson10's live Netty WebFlux server on a random local port and bombards
 * POST /api/v1/trading/orders with concurrent non-blocking WebClient requests to
 * measure real TCP/JSON serialization TPS, TPM, and p50/p95/p99 HTTP tail latencies.
 *
 * Run via: npm run stress:http
 */
@Tag("stress")
@SpringBootTest(
        classes = Lesson10.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
class WebFluxHttpStressTest {

    @LocalServerPort
    private int port;

    @Test
    void measureLiveWebFluxHttpThroughputAndTailLatency() {
        final int warmupRequests = 500;
        final int benchmarkRequests = 5_000;
        final int maxConcurrentClients = 64;

        WebClient client = WebClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();

        Lesson10.OrderRequest sampleOrder = new Lesson10.OrderRequest(
                "Nditah", "QUANT_ALPHA", "LSEG.L", 94200L
        );

        // 1. Warmup Netty event loops, Jackson JSON codecs, and JIT
        runHttpBurst(client, sampleOrder, warmupRequests, maxConcurrentClients, null);

        // 2. Measured HTTP POST burst over loopback TCP
        long[] latenciesNs = new long[benchmarkRequests];
        long startNs = System.nanoTime();

        long successCount = runHttpBurst(client, sampleOrder, benchmarkRequests, maxConcurrentClients, latenciesNs);

        long totalDurationNs = System.nanoTime() - startNs;
        double durationSec = totalDurationNs / 1_000_000_000.0;
        double tps = successCount / durationSec;
        double tpm = tps * 60.0;

        Arrays.sort(latenciesNs);
        double p50Ms = latenciesNs[(int) (benchmarkRequests * 0.50)] / 1_000_000.0;
        double p95Ms = latenciesNs[(int) (benchmarkRequests * 0.95)] / 1_000_000.0;
        double p99Ms = latenciesNs[(int) (benchmarkRequests * 0.99)] / 1_000_000.0;

        System.out.printf("""
                
                ================ LIVE NETTY HTTP STRESS REPORT ================
                Endpoint          : POST /api/v1/trading/orders
                Concurrent Clients: %d
                Total HTTP Orders : %,d (Succeeded: %,d)
                Elapsed Time      : %.3f s
                Throughput (TPS)  : %,.0f req/sec
                Throughput (TPM)  : %,.0f req/min
                Latency p50       : %.2f ms
                Latency p95       : %.2f ms
                Latency p99       : %.2f ms
                ===============================================================
                """, maxConcurrentClients, benchmarkRequests, successCount, durationSec, tps, tpm, p50Ms, p95Ms, p99Ms);

        assertEquals(benchmarkRequests, successCount);
        assertTrue(tps > 500, "Expected > 500 HTTP POST req/sec over loopback Netty");
    }

    private long runHttpBurst(
            WebClient client,
            Lesson10.OrderRequest payload,
            int totalRequests,
            int concurrency,
            long[] latenciesNs
    ) {
        LongAdder okCounter = new LongAdder();

        Flux.range(0, totalRequests)
                .flatMap(i -> Mono.defer(() -> {
                    long t0 = (latenciesNs != null) ? System.nanoTime() : 0L;
                    return client.post()
                            .uri("/api/v1/trading/orders")
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(payload)
                            .retrieve()
                            .toBodilessEntity()
                            .doOnSuccess(res -> {
                                if (res.getStatusCode().is2xxSuccessful()) {
                                    okCounter.increment();
                                }
                                if (latenciesNs != null) {
                                    latenciesNs[i] = System.nanoTime() - t0;
                                }
                            });
                }), concurrency)
                .blockLast();

        return okCounter.sum();
    }
}
