
# Reactive & Concurrent Java Programming for High-Performance Computing



[![Java 25 LTS](https://img.shields.io/badge/Java-25%20LTS-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/25/)
[![Spring Boot 4.1.2](https://img.shields.io/badge/Spring%20Boot-4.1.2-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Project Reactor](https://img.shields.io/badge/Project%20Reactor-WebFlux%20%7C%20Mono%20%7C%20Flux-6DB33F?style=for-the-badge&logo=spring&logoColor=white)](https://projectreactor.io/)
[![Maven Build](https://img.shields.io/badge/Build-Maven%203.9%2B-C71A36?style=for-the-badge&logo=apachemaven&logoColor=white)](https://maven.apache.org/)
[![JUnit 5 & StepVerifier](https://img.shields.io/badge/Tests-JUnit%205%20%7C%20StepVerifier-25A162?style=for-the-badge&logo=junit5&logoColor=white)](https://junit.org/junit5/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)
[![CI Build & Test](https://github.com/Nditah/Reactive-Java/actions/workflows/ci.yml/badge.svg)](https://github.com/Nditah/Reactive-Java/actions/workflows/ci.yml)


A hands-on, 12-lesson workshop teaching **Project Reactor (`Mono`, `Flux`, `ParallelFlux`)**, **Spring WebFlux**, and **Java 25 Concurrency (`Virtual Threads`, `ScopedValue`, Lock-Free CAS)** through real-world **electronic trading, low-latency order books, and banking settlement** use cases.


---

## Tech Stack & Prerequisites

* **Java 25 LTS** (Eclipse Temurin `25.0.5.1+2-tem` recommended via SDKMAN!)
* **Spring Boot 4.1.2** (`spring-boot-starter-webflux`, `spring-boot-starter-test`)
* **Project Reactor** (`reactor-core`, `reactor-test` with `StepVerifier` & `VirtualTimeScheduler`)
* **Apache Maven 3.9+** (wrapped with a JS-friendly `package.json` task runner)

### Switching to Java 25 via SDKMAN! (macOS/Linux)

```bash
sdk install java 25.0.5.1+2-tem
sdk default java 25.0.5.1+2-tem
java -version
```

---

## Project Structure

```text
reactive-coding/
├── LICENSE                                                 # Apache License 2.0
├── pom.xml                                                 # Spring Boot 4.1.2 + Java 25 build config
├── package.json                                            # JS-friendly npm task runner wrapper for Maven
├── README.md                                               # Workshop guide & verification cheat sheet
├── .vscode/
│   └── launch.json                                         # VS Code remote & one-click F5 debug configs
├── src/main/java/com/telixia/reactivecoding/
│   ├── model/
│   │   ├── TraderOrder.java                                # Immutable limit order record (minor-unit pricing)
│   │   └── MarketTick.java                                 # Real-time exchange market tick record
│   ├── feed/
│   │   ├── MarketDataFeed.java                             # Synchronous pull-based streams (Lesson 01)
│   │   └── ReactiveMarketFeed.java                         # Non-blocking Flux & Mono publishers (Lessons 02–10)
│   ├── Lesson01.java                                       # Pull-Based Streams Baseline & Order Book Joins
│   ├── Lesson02.java                                       # Mono, Flux, Lazy Execution & Cold vs. Hot Feeds
│   ├── Lesson03.java                                       # map vs. flatMap vs. concatMap vs. switchMap
│   ├── Lesson04.java                                       # Combining Streams: Mono.zip, merge & combineLatest
│   ├── Lesson05.java                                       # Backpressure & Overload Protection (14:30 UK Spike)
│   ├── Lesson06.java                                       # Concurrency, Schedulers & Isolating Blocking JDBC
│   ├── Lesson07.java                                       # Time, Windowing (VWAP) & Hybrid Micro-Batching
│   ├── Lesson08.java                                       # Resilience: Timeouts, Jitter Back-Off & Live Frankfurter FX API
│   ├── Lesson09.java                                       # Reactive Context Propagation Across Thread Pools
│   ├── Lesson10.java                                       # Capstone: Live WebFlux SSE Trading Blotter
│   ├── Lesson11.java                                       # ParallelFlux CPU Rails, Bulkheads & Java 25 Virtual Threads
│   └── Lesson12.java                                       # Lock-Free CAS Order Book State & Idempotent Executions
└── src/test/java/com/telixia/reactivecoding/
    ├── Lesson09Test.java                                   # Deterministic StepVerifier & Virtual Time Tests
    ├── Lesson10Test.java                                   # WebTestClient REST & Live SSE Streaming Tests
    ├── Lesson11Test.java                                   # ParallelFlux, Bulkhead & Java 25 Virtual Thread Tests
    ├── Lesson12Test.java                                   # High-Contention CAS Invariant & Idempotency Tests
    ├── OrderEngineStressTest.java                          # In-JVM 1M-Tx Throughput (TPS/TPM) & p99 Latency Benchmark
    └── WebFluxHttpStressTest.java                          # Live Netty HTTP 5k-Request Load & p99 Latency Benchmark

```

---

## Quick Cheat Sheet to Verify Everything

| Task                                           | Command                                                                                                                                                                                           |
| :--------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Install / sync Maven JARs**            | `npm install`                                                                                                                                                                                   |
| **Clean compile all 12 lessons**         | `npm run build`                                                                                                                                                                                 |
| **Run CLI lessons (01–09, 11–12)**     | `npm run lesson01` … `npm run lesson12`                                                                                                                                                      |
| **Run JUnit 5 & `StepVerifier` tests** | `npm test`                                                                                                                                                                                      |
| **Start Lesson 10 WebFlux Server**       | `npm start`                                                                                                                                                                                     |
| **Test Live SSE Market Feed (`curl`)** | `curl -N http://localhost:8080/api/v1/trading/ticks/stream`                                                                                                                                     |
| **Test Live Order Blotter (`curl`)**   | `curl -N http://localhost:8080/api/v1/trading/blotter/stream`                                                                                                                                   |
| **Submit a Live Order (`curl`)**       | `curl -X POST http://localhost:8080/api/v1/trading/orders -H "Content-Type: application/json" -d '{"traderName":"Nditah","desk":"QUANT_ALPHA","symbol":"LSEG.L","limitPriceMinorUnits":94200}'` |
| **Attach VS Code / IntelliJ Debugger**   | `npm run debug:lesson` (pauses on port `5005` until your IDE attaches)                                                                                                                        |
| **Run All Stress Benchmarks (TPS / TPM / p99)** | `npm run stress` |
| **Run In-JVM 1M-Tx Engine Stress Test** | `npm run stress:engine` |
| **Run Live Netty HTTP 5k-Req Stress Test** | `npm run stress:http` |


 

---

## 12-Lesson Curriculum Overview

| Lesson                 | Core Concept                                                                                                          | Trading / Banking Scenario                                                           |
| :--------------------- | :-------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------- |
| **`Lesson01`** | Pull-based `Stream`, short-circuiting (`findFirst`, `limit`), `flatMap` joins                                 | Order book & exchange match tape audit (`O(N*M)` scan vs. event-driven join)       |
| **`Lesson02`** | `Mono` vs. `Flux`, lazy `.subscribe()`, Cold vs. Hot (`.share()`) publishers                                  | Per-request order lookup vs. live LSE multicast market-data feed                     |
| **`Lesson03`** | `map`, `flatMap`, `concatMap`, `switchMap`                                                                    | Pre-trade margin enrichment & cancelling stale option quote calculations             |
| **`Lesson04`** | `Mono.zip`, `Flux.merge`, `Flux.combineLatest`                                                                  | Multi-venue Best-Execution Router (LSE + Cboe) & live GBP/USD valuation              |
| **`Lesson05`** | `request(n)`, `limitRate`, `onBackpressureDrop`, `onBackpressureLatest`, `onBackpressureBuffer`             | Protecting a slow risk engine during a 14:30 UK US-market-open tick burst            |
| **`Lesson06`** | `subscribeOn`, `publishOn`, `Schedulers.boundedElastic()`, `parallel()`                                       | Protecting Netty event-loop threads by isolating legacy blocking JDBC calls          |
| **`Lesson07`** | `bufferTimeout`, `groupBy`, `window`, `sample`                                                                | Real-time per-symbol 300ms **VWAP** calculation & micro-batch DB persistence   |
| **`Lesson08`** | `timeout`, `Retry.backoff` + jitter, `onErrorResume`, reactive `WebClient`                                    | Dark-pool SLA timeouts, non-retryable rejections & live Frankfurter ECB FX rates     |
| **`Lesson09`** | `ContextView`, `contextWrite`, `StepVerifier.withVirtualTime()`                                                 | Propagating trace IDs across threads & testing 24-hour MiFID II schedules in ms      |
| **`Lesson10`** | `@RestController`, `TEXT_EVENT_STREAM_VALUE` (SSE), `Sinks.Many`                                                | Live WebFlux trading blotter & Project Reactor vs. Java 25 Virtual Threads           |
| **`Lesson11`** | `ParallelFlux`, `flatMap(..., maxConcurrency)`, Java 25 Virtual Threads, `Semaphore`, `ScopedValue` (JEP 506) | Multi-core CPU option pricing rails & Reactor vs. Virtual Threads bulkhead benchmark |
| **`Lesson12`** | `AtomicReference` CAS loops, `LongAdder`, `ConcurrentHashMap.computeIfAbsent` + `.cache()`                    | Lock-free BBO order book updates & idempotent trade execution under retries          |


