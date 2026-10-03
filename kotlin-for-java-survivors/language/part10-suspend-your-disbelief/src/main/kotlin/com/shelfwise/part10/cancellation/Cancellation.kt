package com.shelfwise.part10.cancellation

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull

fun busyWork(millis: Long) {
    val until = System.nanoTime() + millis * 1_000_000
    while (System.nanoTime() < until) { /* CPU work with no suspension point */ }
}

fun main() = runBlocking {
    // Cancellation is cooperative: a loop that never suspends or checks keeps running.
    var stubbornRounds = 0
    val stubborn = launch(Dispatchers.Default) {
        repeat(5) {
            busyWork(20)
            stubbornRounds++
        }
    }
    delay(30)
    stubborn.cancel()
    stubborn.join()
    println("stubborn finished ${stubbornRounds} of 5") // -> stubborn finished 5 of 5

    // ensureActive() (or isActive, or any suspending call) is a cancellation check.
    var politeRounds = 0
    val polite = launch(Dispatchers.Default) {
        repeat(5) {
            ensureActive()
            busyWork(20)
            politeRounds++
        }
    }
    delay(30)
    polite.cancel()
    polite.join()
    println("polite stopped early: ${politeRounds < 5}") // -> polite stopped early: true

    // Cleanup in finally; suspending cleanup needs NonCancellable.
    val indexer = launch {
        try {
            delay(1_000)
        } finally {
            withContext(NonCancellable) {
                delay(10) // would throw immediately in a cancelled coroutine without NonCancellable
                println("index lock released") // -> index lock released
            }
        }
    }
    delay(10)
    indexer.cancel()
    indexer.join()

    // Timeouts: withTimeout throws TimeoutCancellationException; withTimeoutOrNull returns null.
    println(withTimeoutOrNull(50) { delay(500); "quote" }) // -> null
    try {
        withTimeout(50) { delay(500) }
    } catch (e: TimeoutCancellationException) { // catch the timeout, never CancellationException itself
        println("timed out: ${e::class.simpleName}") // -> timed out: TimeoutCancellationException
    }
    println("still active: $isActive") // -> still active: true
}
