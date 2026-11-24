package com.telixia.reactivecoding;

import com.telixia.reactivecoding.model.TraderOrder;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;

/**
 * LESSON 09 TEST SUITE: Deterministic Reactive Verification with StepVerifier
 *
 * WHY WE NEVER USE .block() OR Thread.sleep() IN REACTIVE TESTS:
 * 1. Calling .block() hides backpressure bugs and deadlocks when testing single-threaded schedulers.
 * 2. Sleeping for real durations (e.g., 24 hours for EOD regulatory reports) is impossible in CI.
 * 3. StepVerifier manipulates Reactor's VirtualTimeScheduler so a 24-hour schedule executes
 *    deterministically in < 50 milliseconds!
 */
class Lesson09Test {

    @Test
    void shouldPropagateCorrelationIdAcrossThreadsViaContext() {
        TraderOrder order = new TraderOrder(2, "Marcus", "STAT_ARB", "BARC.L", 21420L);

        Mono<String> pipeline = Lesson09.executeAuditedOrder(order)
                .contextWrite(Context.of(
                        Lesson09.CORRELATION_ID_KEY, "TRD-TEST-999",
                        Lesson09.DESK_ID_KEY, "STAT_ARB_DESK"
                ));

        StepVerifier.create(pipeline)
                .expectNext("EXECUTED[BARC.L @ 21420p]")
                .verifyComplete();
    }

    @Test
    void shouldVerify24HourRegulatoryScheduleInMillisecondsUsingVirtualTime() {
        // WHY: Lesson09.twentyFourHourRegulatoryBatch() emits 1 report every 6 hours (24 hours total).
        // StepVerifier.withVirtualTime() replaces the clock with VirtualTimeScheduler.
        StepVerifier.withVirtualTime(Lesson09::twentyFourHourRegulatoryBatch)
                .expectSubscription()
                // Assert nothing is emitted before the first 6-hour mark
                .expectNoEvent(Duration.ofHours(6))
                .expectNext("MIFID_II_REPORT_BATCH_#1")
                // Fast-forward another 12 hours -> expect batch #2 and batch #3
                .thenAwait(Duration.ofHours(12))
                .expectNext("MIFID_II_REPORT_BATCH_#2", "MIFID_II_REPORT_BATCH_#3")
                // Fast-forward final 6 hours -> expect batch #4 and stream completion
                .thenAwait(Duration.ofHours(6))
                .expectNext("MIFID_II_REPORT_BATCH_#4")
                .verifyComplete();
    }

    @Test
    void shouldVerifyBackpressureDemandAndCancellation() {
        Flux<Integer> tickSequence = Flux.range(1, 100);

        // Request only 2 items, assert exact values, then cancel upstream cleanly
        StepVerifier.create(tickSequence, 2)
                .expectNext(1, 2)
                .thenRequest(1)
                .expectNext(3)
                .thenCancel()
                .verify();
    }
}
