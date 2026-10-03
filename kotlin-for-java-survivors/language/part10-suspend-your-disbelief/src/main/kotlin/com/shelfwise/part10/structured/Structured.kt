package com.shelfwise.part10.structured

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import java.io.IOException

suspend fun quote(supplier: String, ms: Long): Long {
    delay(ms)
    if (supplier == "flaky") throw IOException("$supplier: HTTP 503")
    return ms * 100
}

// coroutineScope: all children or nothing. One failure cancels the siblings and rethrows.
suspend fun allQuotes(): List<Long> = coroutineScope {
    val a = async { quote("fast", 50) }
    val b = async { quote("flaky", 100) }
    val c = async {
        try {
            quote("slow", 300)
        } finally {
            println("slow quote cancelled") // runs when the failing sibling cancels it
        }
    }
    listOf(a.await(), b.await(), c.await())
}

fun main(): Unit = runBlocking { // ": Unit": the last expression (a Job) is not main's result
    try {
        allQuotes()
    } catch (e: IOException) {
        println("failed: ${e.message}")
    }
    // -> slow quote cancelled
    // -> failed: flaky: HTTP 503

    // supervisorScope: a child's failure stays with that child. Nobody catches the flaky one here;
    // its exception goes to the handler, and its siblings carry on.
    val report = CoroutineExceptionHandler { _, e -> println("reported: ${e.message}") }
    supervisorScope {
        launch { printQuote("fast", 50) }
        launch(report) { printQuote("flaky", 100) }
        launch { printQuote("slow", 150) }
    }
    // -> fast: 5000
    // -> reported: flaky: HTTP 503
    // -> slow: 15000
}

suspend fun printQuote(supplier: String, ms: Long) = println("$supplier: ${quote(supplier, ms)}")
