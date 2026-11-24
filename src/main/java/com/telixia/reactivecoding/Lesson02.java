package com.telixia.reactivecoding;

import com.telixia.reactivecoding.feed.ReactiveMarketFeed;
import com.telixia.reactivecoding.model.MarketTick;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * LESSON 02: Reactive Foundations — Mono, Flux, Lazy Execution & Cold vs. Hot Feeds
 *
 * LEARNING OBJECTIVES:
 * 1. Contrast Mono<T> (0..1) and Flux<T> (0..N) with JavaScript Promises and Java Streams.
 * 2. Prove the golden rule of Project Reactor: "Nothing happens until you subscribe()."
 * 3. Understand why non-blocking Reactor timers run on daemon threads (requiring a latch in main()).
 * 4. Distinguish Cold Publishers (per-subscriber replay, e.g., DB query) from Hot Publishers
 *    (shared live broadcast, e.g., exchange market-data multicast feed).
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * Unlike a JavaScript Promise—which starts executing eagerly the moment it is constructed—
 * a Reactor Mono or Flux is a lazy blueprint. Every .subscribe() creates an independent
 * execution unless the publisher is explicitly shared/multicast (.share() / Sinks).
 */
public class Lesson02 {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("\n=== 1. LAZY EXECUTION: Building a pipeline does ZERO work ===");
        // WHY: In JS, `fetch('/orders/1')` fires immediately. In Reactor, defining a Mono
        // only assembles an immutable operator chain. No network call or timer starts yet.
        Mono<String> lazyOrderLookup = ReactiveMarketFeed.findFirstOrderByInstrumentId(1)
                .doOnSubscribe(sub -> System.out.println("  [Signal] Subscription started!"))
                .map(order -> "Confirmed Order: " + order.traderName() + " -> " + order.symbol());

        System.out.println("Pipeline defined. Notice [Signal] has NOT printed yet!");
        Thread.sleep(300);

        System.out.println("Now calling .subscribe()...");
        CountDownLatch latch1 = new CountDownLatch(1);
        lazyOrderLookup.subscribe(
                result -> System.out.println("  [onNext] " + result),
                error -> System.err.println("  [onError] " + error),
                () -> {
                    System.out.println("  [onComplete] Lookup finished.");
                    latch1.countDown();
                }
        );
        // WHY LATCH IN MAIN(): Reactor's delayElement() runs on daemon scheduler threads.
        // Without waiting, main() would exit and terminate the JVM before the async callback fires.
        // (In Spring WebFlux servers, Netty keeps the JVM alive; you NEVER block in production!)
        latch1.await();

        System.out.println("\n=== 2. COLD PUBLISHER: Independent execution per subscriber ===");
        // WHY: Each subscriber gets its own private stream from element 0—just like querying
        // historical trades from Postgres or replaying a Kafka partition from offset 0.
        Flux<Integer> coldMatchedIds = ReactiveMarketFeed.matchedInstrumentIdsFlux()
                .take(4); // Take first 4 events (0, 2, 4, 6)

        CountDownLatch coldLatch = new CountDownLatch(2);

        coldMatchedIds.subscribe(
                id -> System.out.println("  [Algo-A] Received match ID: " + id),
                Throwable::printStackTrace,
                coldLatch::countDown
        );

        // Algo-B subscribes 250ms later, yet STILL receives 0, 2, 4, 6 from the beginning!
        Thread.sleep(250);
        coldMatchedIds.subscribe(
                id -> System.out.println("  [Algo-B (late)] Received match ID: " + id),
                Throwable::printStackTrace,
                coldLatch::countDown
        );
        coldLatch.await();

        System.out.println("\n=== 3. HOT PUBLISHER: Live Exchange Market-Data Broadcast ===");
        // WHY: A live LSE tick feed does NOT rewind when a new trading desk connects.
        // Calling .share() converts a cold Flux into a hot multicast publisher (refCount = 1).
        // Late subscribers only see ticks emitted AFTER they connect.
        Flux<MarketTick> hotLseFeed = ReactiveMarketFeed.liveLseTickStream(Duration.ofMillis(120))
                .share();

        CountDownLatch hotLatch = new CountDownLatch(2);

        // Desk 1 connects at t = 0ms
        hotLseFeed.take(5).subscribe(
                tick -> System.out.println("  [Desk-1] " + tick.symbol() + " @ " + tick.priceMinorUnits() + "p"),
                Throwable::printStackTrace,
                hotLatch::countDown
        );

        // Desk 2 connects 300ms later—misses the first ~2 ticks and joins the live stream mid-flight
        Thread.sleep(300);
        hotLseFeed.take(3).subscribe(
                tick -> System.out.println("  [Desk-2 (joined late)] " + tick.symbol() + " @ " + tick.priceMinorUnits() + "p"),
                Throwable::printStackTrace,
                hotLatch::countDown
        );
        hotLatch.await();

        System.out.println("\nLesson 02 complete.");
        // Cleanly shut down Reactor's daemon worker pools so Maven exec:java exits immediately
        Schedulers.shutdownNow();
    }
}

/*
Notice in Section 2 that Algo-B receives 0, 2, 4, 6 from the start despite subscribing 250ms late, whereas in Section 3 Desk-2 joins the live LSE feed mid-flight and only sees ticks emitted after it subscribed.
*/