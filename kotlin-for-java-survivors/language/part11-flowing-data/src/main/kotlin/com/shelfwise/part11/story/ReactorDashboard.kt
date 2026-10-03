package com.shelfwise.part11.story

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

data class StockLevel(val store: String, val sku: String, val units: Int)

// The scanner gateway's client. On day two, Mumbai's gateway started sending "12O" (a letter O).
fun fetchLevel(store: String, tick: Long): Mono<StockLevel> = Mono.fromCallable {
    val raw = if (store == "MUM-002" && tick == 1L) "12O" else "${120 - tick * 7}"
    StockLevel(store, "SHW-1001", raw.toInt())
}

// Kabir's first instinct, written the WebFlux way. Every step is an operator.
fun liveStock(store: String): Flux<String> =
    Flux.interval(Duration.ofMillis(10))
        .take(3)
        .flatMap { tick -> fetchLevel(store, tick) }
        .map { "${it.store} ${it.sku}: ${it.units}" }
        .onErrorResume { Flux.empty() } // "a bad reading shouldn't take the dashboard down"

fun main() {
    for (store in listOf("PUN-014", "MUM-002")) {
        liveStock(store)
            .doOnNext(::println)
            .doOnComplete { println("$store: completed") }
            .blockLast()
    }
    // -> PUN-014 SHW-1001: 120
    // -> PUN-014 SHW-1001: 113
    // -> PUN-014 SHW-1001: 106
    // -> PUN-014: completed
    // -> MUM-002 SHW-1001: 120
    // -> MUM-002: completed
}
