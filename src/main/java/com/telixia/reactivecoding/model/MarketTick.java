package com.telixia.reactivecoding.model;

import java.time.Instant;

/**
 * Represents a real-time price tick emitted by an exchange (e.g., LSE, Cboe, or Binance).
 * Used across Lessons 02 to 10 for reactive streaming, backpressure, and VWAP windowing.
 */
public record MarketTick(
        int instrumentId,
        String symbol,
        long priceMinorUnits,
        long volume,
        String venue,
        Instant timestamp
) {
}
