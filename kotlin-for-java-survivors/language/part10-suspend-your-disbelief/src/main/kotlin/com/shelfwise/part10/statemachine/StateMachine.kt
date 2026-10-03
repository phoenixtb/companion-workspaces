package com.shelfwise.part10.statemachine

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

suspend fun fetchPrice(sku: String): Long {
    delay(10)
    return if (sku == "SHW-1001") 16_500 else 72_000
}

suspend fun fetchStock(sku: String): Int {
    delay(10)
    return if (sku == "SHW-1001") 120 else 35
}

// Two suspension points. The compiler turns this function into a state machine with three states.
suspend fun shelfLine(sku: String): String {
    val price = fetchPrice(sku) // state 0 -> suspends, resumes in state 1
    val units = fetchStock(sku) // state 1 -> suspends, resumes in state 2
    return "$sku ₹${price / 100} x$units" // state 2
}

fun main() = runBlocking {
    println(shelfLine("SHW-1001")) // -> SHW-1001 ₹165 x120
}
