package com.telixia.reactivecoding.feed;

import com.telixia.reactivecoding.model.TraderOrder;
import java.util.stream.Stream;

/**
 * Synchronous, pull-based data fixtures simulating exchange order books and match feeds.
 *
 * WHY THIS EXISTS:
 * Before introducing Project Reactor (Flux/Mono), Lesson 01 establishes a baseline using
 * standard java.util.stream.Stream. Each method returns a fresh Stream because Java Streams
 * are single-use iterators—consuming one twice throws IllegalStateException.
 */
public final class MarketDataFeed {

    private MarketDataFeed() {
        // Utility class: prevent instantiation
    }

    /**
     * Stream of FTSE 100 ticker symbols from a consolidated market tape.
     */
    public static Stream<String> tickerSymbolsStream() {
        return Stream.of(
                "HSBA.L", "BARC.L", "VOD.L", "AZN.L", "SHEL.L",
                "ULVR.L", "BP.L", "GSK.L", "RIO.L", "LSEG.L"
        );
    }

    /**
     * Stream of matched instrument IDs emitted by an exchange matching engine (0, 2, 4, ..., 18).
     */
    public static Stream<Integer> matchedInstrumentIdsStream() {
        return Stream.iterate(0, id -> id + 2)
                .limit(10);
    }

    /**
     * Stream of active limit orders resting in the order book.
     *
     * NOTE FOR INTERVIEWS:
     * Instrument ID 2 intentionally appears twice (Marcus and Elena) to represent two
     * competing bids at the same price level. This tests 1-to-many stream joins via flatMap.
     */
    public static Stream<TraderOrder> orderBookStream() {
        return Stream.of(
                new TraderOrder(1, "Amara", "EQUITIES_ALGO", "HSBA.L", 68550L),
                new TraderOrder(2, "Marcus", "STAT_ARB", "BARC.L", 21420L),
                new TraderOrder(2, "Elena", "MARKET_MAKING", "BARC.L", 21425L),
                new TraderOrder(4, "Kenji", "FX_SPOT", "AZN.L", 1184000L),
                new TraderOrder(5, "Sofia", "DELTA_ONE", "SHEL.L", 279050L),
                new TraderOrder(6, "Liam", "EXECUTION_DESK", "ULVR.L", 412300L)
        );
    }
}
