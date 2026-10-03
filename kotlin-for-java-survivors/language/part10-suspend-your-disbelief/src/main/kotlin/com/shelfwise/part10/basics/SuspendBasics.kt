package com.shelfwise.part10.basics

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

// A suspend function can pause without blocking its thread. delay() suspends; Thread.sleep() would block.
suspend fun quote(supplier: Int): Long {
    delay(200)
    return 16_000L + supplier * 10
}

fun main() = runBlocking { // bridges blocking code (main) and suspending code; never use it inside coroutines
    // launch: fire and forget, returns a Job.
    val job = launch { println("indexing started") }
    job.join() // -> indexing started

    // async: returns a Deferred<T>; await() suspends until the value is ready.
    val first = async { quote(1) }
    val second = async { quote(2) }
    println("best ${minOf(first.await(), second.await())}") // -> best 16010

    // 10,000 concurrent waits, and how many threads did the waiting.
    val threads = ConcurrentHashMap.newKeySet<String>()
    val done = AtomicInteger()
    (1..10_000).map { supplier ->
        async {
            threads += Thread.currentThread().name.substringBefore(" @") // strip the debug-mode coroutine name
            quote(supplier)
            done.incrementAndGet()
        }
    }.awaitAll()
    println("${done.get()} quotes on ${threads.size} thread") // -> 10000 quotes on 1 thread
}
