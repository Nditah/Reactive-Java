package com.telixia.reactivecoding.model;

/**
 * Immutable domain model representing a limit order resting in an order book.
 *
 * WHY A RECORD:
 * 1. Thread-safety by construction: Reactive pipelines hop across Netty event-loop
 *    and worker threads. Immutable records eliminate data races without locks.
 * 2. Value semantics: Automatically generates equals(), hashCode(), and toString().
 *
 * WHY LONG MINOR UNITS (PENCE/CENTS) INSTEAD OF BIGDECIMAL:
 * On ultra-low-latency trading hot paths, BigDecimal allocates new objects on every
 * arithmetic step, creating Garbage Collection (GC) pauses. Storing prices in minor
 * units (e.g., 68550L = £685.50) keeps hot paths primitive and allocation-light.
 */
public record TraderOrder(
        int instrumentId,
        String traderName,
        String desk,
        String symbol,
        long limitPriceMinorUnits
) {
    public TraderOrder {
        // Fail fast: make illegal financial states unrepresentable at construction time.
        if (limitPriceMinorUnits <= 0) {
            throw new IllegalArgumentException("Limit price must be positive, got: " + limitPriceMinorUnits);
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Ticker symbol must not be blank");
        }
    }
}
