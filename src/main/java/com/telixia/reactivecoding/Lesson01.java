package com.telixia.reactivecoding;

import com.telixia.reactivecoding.feed.MarketDataFeed;
import com.telixia.reactivecoding.model.TraderOrder;

/**
 * LESSON 01: Pull-Based Streams Baseline & Order Book Joins
 *
 * LEARNING OBJECTIVES:
 * 1. Understand lazy evaluation and vertical (element-by-element) stream execution.
 * 2. Use short-circuiting operations (findFirst, limit) to avoid unnecessary CPU work.
 * 3. Compare O(N * M) nested filtering against event-driven flatMap joins.
 * 4. Identify why pull-based Java Streams fall short for live market-data firehoses
 *    (setting up Lesson 02 with Project Reactor).
 *
 * SENIOR INTERVIEW TAKEAWAY:
 * Java Streams are PULL-based and single-use. Intermediate operations (filter, map, skip)
 * do ZERO work until a terminal operation (forEach, findFirst, toList) pulls data through
 * the pipeline. However, Java Streams have no native concept of asynchronous time,
 * non-blocking I/O, or backpressure when a producer emits faster than a consumer can process.
 */
public class Lesson01 {

    public static void main(String[] args) {

        System.out.println("\n=== 1. TAPE AUDIT: Print all matched instrument IDs ===");
        // WHY: forEach is a terminal operation that triggers execution of the pipeline.
        MarketDataFeed.matchedInstrumentIdsStream()
                .forEach(id -> System.out.println("Matched Instrument ID: " + id));

        System.out.println("\n=== 2. PRIORITY ROUTING: Filter matched IDs below threshold (< 5) ===");
        // WHY: Predicates should be stateless and side-effect free so the pipeline remains predictable.
        MarketDataFeed.matchedInstrumentIdsStream()
                .filter(id -> id < 5)
                .forEach(id -> System.out.println("Fast-Lane Instrument ID: " + id));

        System.out.println("\n=== 3. TICK SAMPLING: 2nd and 3rd matched IDs greater than 5 ===");
        // WHY ORDER MATTERS:
        // filter(id > 5) runs first -> [6, 8, 10, 12, 14, 16, 18]
        // skip(1) drops 6 -> [8, 10, 12, ...]
        // limit(2) short-circuits after pulling 8 and 10; 12..18 are NEVER evaluated.
        MarketDataFeed.matchedInstrumentIdsStream()
                .filter(id -> id > 5)
                .skip(1)
                .limit(2)
                .forEach(id -> System.out.println("Sampled Tick ID: " + id));

        System.out.println("\n=== 4. SHORT-CIRCUIT LOOKUP: First matched ID > 5 (or -1) ===");
        // WHY: Elements travel vertically one at a time through the pipeline.
        // As soon as 6 passes filter(id > 5), findFirst() terminates the stream immediately.
        // Note: orElse(-1) is fine for a constant; use orElseGet(() -> expensiveCall()) for I/O!
        int firstTriggeredId = MarketDataFeed.matchedInstrumentIdsStream()
                .filter(id -> id > 5)
                .findFirst()
                .orElse(-1);
        System.out.println("First Triggered Instrument ID: " + firstTriggeredId);

        System.out.println("\n=== 5. DESK PROJECTION: Extract trader names from resting orders ===");
        // WHY: map() performs a synchronous 1-to-1 transformation using a method reference.
        MarketDataFeed.orderBookStream()
                .map(TraderOrder::traderName)
                .forEach(trader -> System.out.println("Active Trader: " + trader));

        System.out.println("\n=== 6A. FILL NOTIFICATION (Anti-Pattern: Nested Scan) ===");
        // WHY THIS IS SUBOPTIMAL IN TRADING SYSTEMS:
        // For every order in orderBookStream(), we re-open and scan matchedInstrumentIdsStream().
        // That creates O(N * M) complexity and fails completely once the feed becomes a live,
        // infinite event stream.
        MarketDataFeed.orderBookStream()
                .filter(order -> MarketDataFeed.matchedInstrumentIdsStream()
                        .anyMatch(matchedId -> matchedId == order.instrumentId()))
                .forEach(order -> System.out.println("Fill Alert (Scan) -> "
                        + order.traderName() + " [" + order.symbol() + "]"));

        System.out.println("\n=== 6B. FILL NOTIFICATION (Event-Driven flatMap Join) ===");
        // WHY THIS IS THE REACTIVE MENTAL MODEL:
        // Drive the pipeline from the incoming exchange match event stream.
        // For each incoming matchedId, flatMap fans out to matching resting orders (1-to-many)
        // and flattens the resulting Stream<Stream<TraderOrder>> into a single Stream<TraderOrder>.
        // Notice both Marcus and Elena (Instrument ID 2) are notified cleanly.
        MarketDataFeed.matchedInstrumentIdsStream()
                .flatMap(matchedId -> MarketDataFeed.orderBookStream()
                        .filter(order -> order.instrumentId() == matchedId))
                .forEach(order -> System.out.println("Fill Alert (Event-Driven) -> "
                        + order.traderName() + " [" + order.desk() + " | "
                        + order.symbol() + " @ " + order.limitPriceMinorUnits() + "p]"));
    }
}
