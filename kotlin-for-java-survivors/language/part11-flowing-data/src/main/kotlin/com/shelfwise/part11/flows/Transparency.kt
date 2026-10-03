package com.shelfwise.part11.flows

import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // A Java-style try/catch around emit also catches the COLLECTOR's exceptions.
    // Emitting again after that breaks the flow's contract.
    val readings = flow {
        try {
            emit(1)
        } catch (e: IllegalStateException) {
            emit(-1) // "fallback"
        }
    }
    try {
        readings.collect { if (it == 1) throw IllegalStateException("display offline") }
    } catch (e: IllegalStateException) {
        println(e.message!!.lineSequence().first()) // -> Flow exception transparency is violated:
    }

    // catch { } only sees upstream exceptions; the collector's own failure goes straight to the caller.
    try {
        flow { emit(1) }
            .catch { println("never reached") }
            .collect { throw IllegalStateException("display offline") }
    } catch (e: IllegalStateException) {
        println("collector failed: ${e.message}") // -> collector failed: display offline
    }
}
