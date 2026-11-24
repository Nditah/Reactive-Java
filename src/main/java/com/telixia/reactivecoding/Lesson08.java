package com.telixia.reactivecoding;

import io.netty.resolver.DefaultAddressResolverGroup;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.HttpResources;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LESSON 08: Resilience, Timeouts, Exponential Back-Off & Live Public FX Market APIs
 *
 * LEARNING OBJECTIVES:
 * 1. Enforce strict SLA latency budgets using .timeout(Duration) so slow liquidity venues
 *    never cause cascading thread/memory exhaustion.
 * 2. Implement transient fault recovery using .retryWhen(Retry.backoff(...).jitter(0.5))
 *    and distinguish retryable network blips from non-retryable business rejections.
 * 3. Provide deterministic fallback prices via .onErrorResume() when a primary venue degrades.
 * 4. Call a live public financial API (Frankfurter ECB FX spot rates) non-blockingly using
 *    Spring's reactive WebClient with DefaultAddressResolverGroup, timeout, and fallback protection.
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * - Never use unbounded .retry() in production: if a downstream exchange or ledger is overloaded,
 *   immediate synchronized retries from 500 pods create a "thundering herd" DDoS attack against
 *   your own infrastructure. Always use exponential back-off + randomized jitter + maxAttempts.
 * - Never retry 4xx business errors (e.g., InsufficientFundsException or OrderRejectedException);
 *   use .filter(ex -> ex instanceof TransientVenueException) inside Retry.backoff().
 */

public class Lesson08 {

    static class TransientVenueException extends RuntimeException {
        TransientVenueException(String msg) {
            super(msg);
        }
    }

    static class OrderRejectedException extends RuntimeException {
        OrderRejectedException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. STRICT SLA TIMEOUT + FALLBACK PRICE (onErrorResume) ===");
        CountDownLatch latch1 = new CountDownLatch(1);
        querySlowLiquidityVenue("HSBA.L", 400)
                .timeout(Duration.ofMillis(150))
                .onErrorResume(TimeoutException.class, ex -> {
                    System.out.println("  [SLA Breach] Primary venue timed out after 150ms -> routing to Backup Cache");
                    return Mono.just("HSBA.L @ 68540p [SOURCE=CACHED_LSE_CLOSE]");
                })
                .subscribe(
                        quote -> System.out.println("  [Resolved Quote] " + quote),
                        Throwable::printStackTrace,
                        latch1::countDown
                );
        latch1.await();

        System.out.println("\n=== 2. EXPONENTIAL BACK-OFF + JITTER FOR TRANSIENT NETWORK BLIPS ===");
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicInteger attemptCounter = new AtomicInteger(0);

        Mono.defer(() -> {
                    int attempt = attemptCounter.incrementAndGet();
                    System.out.println("  [Execution Router] Attempt #" + attempt + " sending order to Exchange...");
                    if (attempt < 3) {
                        return Mono.error(new TransientVenueException("FIX gateway socket reset (attempt " + attempt + ")"));
                    }
                    return Mono.just("ACK: Order filled on Attempt #" + attempt + " [BARC.L @ 21420p]");
                })
                .retryWhen(Retry.backoff(3, Duration.ofMillis(80))
                        .jitter(0.5)
                        .filter(err -> err instanceof TransientVenueException)
                        .doBeforeRetry(signal -> System.out.printf("    [Retry Scheduled] retryIndex=%d due to: %s%n",
                                signal.totalRetries() + 1, signal.failure().getMessage())))
                .subscribe(
                        ack -> System.out.println("  [Confirmed] " + ack),
                        Throwable::printStackTrace,
                        latch2::countDown
                );
        latch2.await();

        System.out.println("\n=== 3. NON-RETRYABLE BUSINESS ERROR: Fail Fast Without Retrying ===");
        CountDownLatch latch3 = new CountDownLatch(1);
        AtomicInteger businessAttempts = new AtomicInteger(0);

        Mono.defer(() -> {
                    businessAttempts.incrementAndGet();
                    return Mono.error(new OrderRejectedException("Insufficient margin for 10,000 lots AZN.L"));
                })
                .retryWhen(Retry.backoff(3, Duration.ofMillis(50))
                        .filter(err -> err instanceof TransientVenueException))
                .onErrorResume(OrderRejectedException.class, ex ->
                        Mono.just("REJECTED IMMEDIATELY after " + businessAttempts.get() + " attempt: " + ex.getMessage()))
                .subscribe(
                        status -> System.out.println("  [Risk Engine] " + status),
                        Throwable::printStackTrace,
                        latch3::countDown
                );
        latch3.await();

        System.out.println("\n=== 4. LIVE PUBLIC FX MARKET API (Frankfurter ECB Rates + WebClient) ===");
        HttpClient httpClient = HttpClient.create()
                .resolver(DefaultAddressResolverGroup.INSTANCE);

        WebClient webClient = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl("https://api.frankfurter.dev/v1")
                .build();

        CountDownLatch latch4 = new CountDownLatch(1);
        Flux.just("USD", "EUR", "JPY")
                .flatMap(quoteCurrency -> fetchLiveGbpFxRate(webClient, quoteCurrency))
                .subscribe(
                        spot -> System.out.println("  [Live FX Feed] " + spot),
                        Throwable::printStackTrace,
                        latch4::countDown
                );
        latch4.await();

        System.out.println("\nLesson 08 complete.");
        HttpResources.disposeLoopsAndConnections();
        Schedulers.shutdownNow();
    }

    private static Mono<String> querySlowLiquidityVenue(String symbol, long simulatedDelayMs) {
        return Mono.delay(Duration.ofMillis(simulatedDelayMs))
                .map(ignored -> symbol + " @ 68555p [SOURCE=DARK_POOL_A]");
    }

    @SuppressWarnings("unchecked")
    private static Mono<String> fetchLiveGbpFxRate(WebClient client, String targetCurrency) {
        return client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/latest")
                        .queryParam("base", "GBP")
                        .queryParam("symbols", targetCurrency)
                        .build())
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(1, Duration.ofMillis(200))
                        .jitter(0.3)
                        // Only retry transient network/5xx errors, never 4xx client errors
                        .filter(err -> !(err instanceof WebClientResponseException.BadRequest)))
                .map(response -> {
                    Map<String, Object> rates = (Map<String, Object>) response.get("rates");
                    Object rate = rates.get(targetCurrency);
                    return String.format("GBP/%-3s | Spot Rate: %-8s (Date=%s | LIVE via Frankfurter ECB API)",
                            targetCurrency, rate, response.get("date"));
                })
                .onErrorResume(ex -> {
                    Throwable root = ex.getCause() != null ? ex.getCause() : ex;
                    return Mono.just(String.format("GBP/%-3s | Spot Rate: FALLBACK (Cause: %s - %s)",
                            targetCurrency, root.getClass().getSimpleName(), root.getMessage()));
                });
    }
}


