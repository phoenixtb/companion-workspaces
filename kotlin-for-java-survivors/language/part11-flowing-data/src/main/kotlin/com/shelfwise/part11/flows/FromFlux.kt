package com.shelfwise.part11.flows

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest

// The gateway client as a suspend function: one value, so no Mono. (StockLevel is from ColdFlows.kt.)
suspend fun fetchLevel(store: String, tick: Int): StockLevel {
    delay(5) // the remote call
    val raw = if (store == "MUM-002" && tick == 1) "12O" else "${120 - tick * 7}"
    return StockLevel(store, "SHW-1001", raw.toInt())
}

// The opening's Flux chain, rewritten. Only the timer needed a stream; the rest is a loop that waits.
fun liveStock(store: String): Flow<String> = flow {
    for (tick in 0 until 3) {
        delay(10)
        emit(fetchLevel(store, tick)) // Reactor needed flatMap for this call
    }
}.map { "${it.store} ${it.sku}: ${it.units}" }

// A lookup whose latency depends on the item: later items answer first.
fun slowLookup(n: Int): Flow<Int> = flow {
    delay((4 - n) * 10L)
    emit(n)
}

@OptIn(ExperimentalCoroutinesApi::class) // flatMapMerge and flatMapConcat need opt-in in kotlinx.coroutines 1.11.0
fun main() = runTest {
    liveStock("PUN-014").collect { println(it) }
    // -> PUN-014 SHW-1001: 120
    // -> PUN-014 SHW-1001: 113
    // -> PUN-014 SHW-1001: 106

    // No operator swallows anything by default: the failure reaches the code that collected.
    try {
        liveStock("MUM-002").collect { println(it) }
    } catch (e: NumberFormatException) {
        println("MUM-002 failed: ${e.message}")
    }
    // -> MUM-002 SHW-1001: 120
    // -> MUM-002 failed: For input string: "12O"

    // map with a suspend call: one at a time, in order. This is what most Reactor flatMaps meant.
    println(flowOf(1, 2, 3).map { n -> slowLookup(n).toList().single() }.toList()) // -> [1, 2, 3]
    // flatMapMerge: concurrent, so results arrive in completion order, like Reactor's flatMap.
    println(flowOf(1, 2, 3).flatMapMerge { slowLookup(it) }.toList()) // -> [3, 2, 1]
    // flatMapConcat: one inner flow after another, like Reactor's concatMap.
    println(flowOf(1, 2, 3).flatMapConcat { slowLookup(it) }.toList()) // -> [1, 2, 3]
}
