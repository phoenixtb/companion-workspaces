package com.shelfwise.part11.reactor

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

// A Reactor-based client from another team.
fun legacyPrice(sku: String): Mono<Long> = Mono.just(if (sku == "SHW-1001") 16_500L else 72_000L)

fun legacyStock(): Flux<Int> = Flux.just(120, 113, 106)

fun main() = runBlocking {
    // Reactor -> coroutines: await a Mono, collect a Flux as a Flow.
    println(legacyPrice("SHW-1001").awaitSingle()) // -> 16500
    // An empty Mono ("not found" in most Java code) makes awaitSingle throw; use awaitSingleOrNull.
    println(Mono.empty<Long>().awaitSingleOrNull()) // -> null
    try {
        Mono.empty<Long>().awaitSingle()
    } catch (e: NoSuchElementException) {
        println("awaitSingle on empty: ${e::class.simpleName}") // -> awaitSingle on empty: NoSuchElementException
    }
    println(legacyStock().asFlow().map { it - 100 }.toList()) // -> [20, 13, 6]

    // Coroutines -> Reactor: suspend code behind a Mono or a Flux, for APIs that expect them.
    val price: Mono<String> = mono { "₹${legacyPrice("SHW-2040").awaitSingle() / 100}" }
    println(price.block()) // -> ₹720

    val ticks: Flux<Int> = flux {
        for (i in 1..3) {
            delay(5)
            send(i) // flux { } is a producer: send, not emit
        }
    }
    println(ticks.collectList().block()) // -> [1, 2, 3]

    // A Flow as a Flux, e.g. to return from a WebFlux controller that expects Publisher types.
    println(flow { emit("a"); emit("b") }.asFlux().collectList().block()) // -> [a, b]
}
