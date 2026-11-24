package com.telixia.reactivecoding;

import com.telixia.reactivecoding.model.MarketTick;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;

/**
 * LESSON 04: Combining Streams & Fan-Out / Fan-In (zip, merge, combineLatest)
 *
 * LEARNING OBJECTIVES:
 * 1. Mono.zip(): Fan-out independent non-blocking calls in parallel and join results
 *    (equivalent to Promise.all() in JavaScript).
 * 2. Flux.merge(): Interleave live streams from multiple venues as events arrive in real time.
 * 3. Flux.zip(): Pair elements 1-to-1 by sequence index (lockstep pairing).
 * 4. Flux.combineLatest(): Recompute a derived state whenever ANY upstream source emits a new value
 *    (the core pattern behind live FX pricing & order-book mid-price engines).
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * - Use Mono.zip() to eliminate sequential waterfall latency when fetching independent data
 *   (e.g., Account Balance + FX Spot Rate + Credit Limit simultaneously).
 * - Never confuse Flux.zip() (waits for a 1:1 pair from every source; slows down to the slowest
 *   producer) with Flux.combineLatest() (uses the most recent cached value from each source).
 */
public class Lesson04 {

    public record FxValuation(
            String symbol,
            long gbpPricePence,
            double gbpUsdRate,
            double usdValue
    ) {
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. MONO.ZIP: Parallel Fan-Out / Fan-In (Like Promise.all) ===");
        // WHY: Fetching account balance (180ms), credit limit (220ms), and FX rate (150ms)
        // sequentially takes 550ms. Mono.zip runs all three concurrently in ~220ms!
        CountDownLatch latch1 = new CountDownLatch(1);
        long startFanOut = System.nanoTime();

        Mono<Long> accountBalanceMono = Mono.delay(Duration.ofMillis(180)).map(t -> 250_000_00L); // £250,000.00
        Mono<Long> creditLimitMono = Mono.delay(Duration.ofMillis(220)).map(t -> 1_000_000_00L);  // £1,000,000.00
        Mono<Double> gbpUsdRateMono = Mono.delay(Duration.ofMillis(150)).map(t -> 1.2850);

        Mono.zip(accountBalanceMono, creditLimitMono, gbpUsdRateMono)
                .map(tuple -> {
                    long cashPence = tuple.getT1();
                    long creditPence = tuple.getT2();
                    double fxRate = tuple.getT3();
                    double totalBuyingPowerUsd = ((cashPence + creditPence) / 100.0) * fxRate;
                    return String.format("Total Buying Power: $%,.2f (FX=%.4f)", totalBuyingPowerUsd, fxRate);
                })
                .subscribe(
                        summary -> {
                            long elapsedMs = (System.nanoTime() - startFanOut) / 1_000_000;
                            System.out.println("  [Mono.zip] " + summary + " in " + elapsedMs + "ms");
                        },
                        Throwable::printStackTrace,
                        latch1::countDown
                );
        latch1.await();

        System.out.println("\n=== 2. FLUX.MERGE: Consolidated Multi-Venue Market Tape ===");
        // WHY: An institutional Smart Order Router subscribes to LSE and Cboe Europe simultaneously.
        // Flux.merge() subscribes eagerly to both feeds and emits ticks immediately as they arrive,
        // without waiting for the other venue.
        CountDownLatch latch2 = new CountDownLatch(1);

        Flux<MarketTick> lseFeed = Flux.interval(Duration.ofMillis(90))
                .take(3)
                .map(i -> new MarketTick(1, "HSBA.L", 68550L + i * 5, 1_000L, "LSE", Instant.now()));

        Flux<MarketTick> cboeFeed = Flux.interval(Duration.ofMillis(140))
                .take(3)
                .map(i -> new MarketTick(1, "HSBA.L", 68548L + i * 6, 800L, "CBOE_EU", Instant.now()));

        Flux.merge(lseFeed, cboeFeed)
                .subscribe(
                        tick -> System.out.printf("  [merge] Venue=%-7s | %s @ %dp (vol=%d)%n",
                                tick.venue(), tick.symbol(), tick.priceMinorUnits(), tick.volume()),
                        Throwable::printStackTrace,
                        latch2::countDown
                );
        latch2.await();

        System.out.println("\n=== 3. FLUX.COMBINELATEST: Live Cross-Currency Valuation Engine ===");
        // WHY: Stock prices tick at one frequency (every 100ms), while GBP/USD FX spot rates tick
        // at a different frequency (every 230ms). combineLatest() recalculates the USD price
        // immediately whenever EITHER the equity price OR the FX rate updates.
        CountDownLatch latch3 = new CountDownLatch(1);

        Flux<Long> hsbaPriceStream = Flux.just(68500L, 68520L, 68590L, 68610L)
                .delayElements(Duration.ofMillis(100));

        Flux<Double> fxSpotStream = Flux.just(1.2840, 1.2855)
                .delayElements(Duration.ofMillis(230));

        Flux.combineLatest(
                        hsbaPriceStream,
                        fxSpotStream,
                        (pricePence, fxRate) -> new FxValuation(
                                "HSBA.L",
                                pricePence,
                                fxRate,
                                (pricePence / 100.0) * fxRate
                        )
                )
                .subscribe(
                        val -> System.out.printf("  [combineLatest] %s: %dp * %.4f = $%.4f%n",
                                val.symbol(), val.gbpPricePence(), val.gbpUsdRate(), val.usdValue()),
                        Throwable::printStackTrace,
                        latch3::countDown
                );
        latch3.await();

        System.out.println("\nLesson 04 complete.");
        // Cleanly shut down Reactor's daemon worker pools so Maven exec:java exits immediately
        Schedulers.shutdownNow();
    }
}
