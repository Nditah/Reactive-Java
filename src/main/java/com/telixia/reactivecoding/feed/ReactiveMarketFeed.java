package com.telixia.reactivecoding.feed;

import com.telixia.reactivecoding.model.MarketTick;
import com.telixia.reactivecoding.model.TraderOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Non-blocking, asynchronous market-data and order-book publishers using Project Reactor.
 *
 * WHY THIS EXISTS:
 * In Lesson 01, MarketDataFeed returned synchronous java.util.stream.Stream instances.
 * Here, ReactiveMarketFeed returns Mono<T> (0 or 1 async result, similar to a lazy JS Promise)
 * and Flux<T> (0 to N async events over time, similar to an Observable / AsyncGenerator with backpressure).
 */
public final class ReactiveMarketFeed {

    private static final List<TraderOrder> ORDER_BOOK = List.of(
            new TraderOrder(1, "Amara", "EQUITIES_ALGO", "HSBA.L", 68550L),
            new TraderOrder(2, "Marcus", "STAT_ARB", "BARC.L", 21420L),
            new TraderOrder(2, "Elena", "MARKET_MAKING", "BARC.L", 21425L),
            new TraderOrder(4, "Kenji", "FX_SPOT", "AZN.L", 1184000L),
            new TraderOrder(5, "Sofia", "DELTA_ONE", "SHEL.L", 279050L),
            new TraderOrder(6, "Liam", "EXECUTION_DESK", "ULVR.L", 412300L)
    );

    private ReactiveMarketFeed() {
    }

    /**
     * Cold Publisher: Emits matched instrument IDs (0, 2, 4, ..., 18) with a simulated
     * network/matching-engine latency of 150ms per event.
     *
     * WHY DELAY ELEMENTS WITHOUT BLOCKING:
     * Flux.delayElements() schedules emissions on Reactor's non-blocking parallel timer threads
     * rather than calling Thread.sleep(), keeping OS threads free for other work.
     */
    public static Flux<Integer> matchedInstrumentIdsFlux() {
        return Flux.range(0, 10)
                .map(i -> i * 2)
                .delayElements(Duration.ofMillis(150));
    }

    /**
     * Cold Publisher: Streams all resting limit orders from the order book with simulated I/O latency.
     */
    public static Flux<TraderOrder> orderBookFlux() {
        return Flux.fromIterable(ORDER_BOOK)
                .delayElements(Duration.ofMillis(100));
    }

    /**
     * Mono<T>: Asynchronous single-value lookup (0 or 1 element) by instrumentId.
     *
     * WHY MONO INSTEAD OF OPTIONAL<T>:
     * Optional<T> assumes the value is already in memory. Mono<T> represents a future
     * non-blocking lookup (e.g., R2DBC database query or Redis cache hit) that may emit
     * 1 item (onNext + onComplete), 0 items (onComplete only), or an error (onError).
     */
    public static Mono<TraderOrder> findFirstOrderByInstrumentId(int instrumentId) {
        return Flux.fromIterable(ORDER_BOOK)
                .filter(order -> order.instrumentId() == instrumentId)
                .next() // Takes the first match and converts Flux<TraderOrder> -> Mono<TraderOrder>
                .delayElement(Duration.ofMillis(200));
    }

    /**
     * Simulated Live Exchange Tick Feed (used in later lessons for burst & backpressure tests).
     */
    public static Flux<MarketTick> liveLseTickStream(Duration interval) {
        String[] symbols = {"HSBA.L", "BARC.L", "AZN.L", "SHEL.L", "ULVR.L"};
        long[] basePrices = {68550L, 21420L, 1184000L, 279050L, 412300L};

        return Flux.interval(interval)
                .map(seq -> {
                    int idx = (int) (seq % symbols.length);
                    long jitter = ThreadLocalRandom.current().nextLong(-25, 26);
                    long volume = ThreadLocalRandom.current().nextLong(100, 5_000);
                    return new MarketTick(
                            idx + 1,
                            symbols[idx],
                            basePrices[idx] + jitter,
                            volume,
                            "LSE",
                            Instant.now()
                    );
                });
    }
}
