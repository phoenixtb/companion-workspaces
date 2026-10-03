package com.shelfwise.part11.flows

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

data class StockLevel(val store: String, val sku: String, val units: Int)

// A cold flow: the block runs once per collector, when it collects. Nothing runs before that.
fun stockLevels(store: String): Flow<StockLevel> = flow {
    println("scanner connected for $store")
    for (tick in 0..3) {
        delay(10) // suspends between values; no thread is held
        emit(StockLevel(store, "SHW-1001", 120 - tick * 7))
    }
}

fun main() = runBlocking {
    val pune = stockLevels("PUN-014") // nothing printed yet: building a flow runs nothing

    // Operators read like the Stream API, but they are suspend-aware and run in the collector's coroutine.
    pune
        .filter { it.units < 115 }
        .map { "${it.store}: ${it.units}" }
        .take(2)
        .collect { println(it) }
    // -> scanner connected for PUN-014
    // -> PUN-014: 113
    // -> PUN-014: 106

    // A second collection runs the producer again.
    println(pune.toList().size)
    // -> scanner connected for PUN-014
    // -> 4

    // flowOn changes where the UPSTREAM runs; the collector stays where it is.
    stockLevels("MUM-002")
        .map { Thread.currentThread().name.startsWith("DefaultDispatcher") }
        .flowOn(Dispatchers.Default)
        .take(1)
        .collect { println("produced on Default: $it") }
    // -> scanner connected for MUM-002
    // -> produced on Default: true

    // Emitting from a different context inside flow { } breaks the flow's contract.
    try {
        flow { withContext(Dispatchers.Default) { emit(1) } }.collect { }
    } catch (e: IllegalStateException) {
        println(e.message!!.substringBefore(":")) // -> Flow invariant is violated
    }
}
