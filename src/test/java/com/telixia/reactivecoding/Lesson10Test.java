package com.telixia.reactivecoding;

import com.telixia.reactivecoding.model.MarketTick;
import com.telixia.reactivecoding.model.TraderOrder;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LESSON 10 TEST SUITE: Non-Blocking HTTP & Server-Sent Events (SSE) Verification
 *
 * Uses WebTestClient bound directly to Lesson10's @RestController (no TCP port binding needed)
 * to verify Mono snapshots, POST order submissions, and infinite Flux SSE streams.
 */
class Lesson10Test {

    private final WebTestClient webTestClient = WebTestClient
            .bindToController(new Lesson10())
            .build();

    @Test
    void shouldReturnFirstRestingOrderSnapshot() {
        webTestClient.get()
                .uri("/api/v1/trading/orders/first?instrumentId=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody(TraderOrder.class)
                .value(order -> {
                    assertEquals(2, order.instrumentId());
                    assertEquals("BARC.L", order.symbol());
                });
    }

    @Test
    void shouldStreamLiveMarketTicksOverSseAndCancelCleanly() {
        Flux<MarketTick> tickStream = webTestClient.get()
                .uri("/api/v1/trading/ticks/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(MarketTick.class)
                .getResponseBody();

        StepVerifier.create(tickStream.take(3))
                .assertNext(tick -> {
                    assertNotNull(tick.symbol());
                    assertTrue(tick.priceMinorUnits() > 0);
                })
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void shouldAcceptOrderSubmissionAndAssignSequenceId() {
        Lesson10.OrderRequest request = new Lesson10.OrderRequest(
                "Nditah", "QUANT_ALPHA", "LSEG.L", 94200L
        );

        webTestClient.post()
                .uri("/api/v1/trading/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(TraderOrder.class)
                .value(created -> {
                    assertTrue(created.instrumentId() > 100);
                    assertEquals("Nditah", created.traderName());
                    assertEquals("LSEG.L", created.symbol());
                    assertEquals(94200L, created.limitPriceMinorUnits());
                });
    }
}
