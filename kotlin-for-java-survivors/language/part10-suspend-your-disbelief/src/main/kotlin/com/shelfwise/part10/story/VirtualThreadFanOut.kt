package com.shelfwise.part10.story

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

// Kabir's second version: one virtual thread per supplier call. Blocking is cheap now.
fun blockingQuote(supplier: Int, completed: AtomicInteger): Long {
    Thread.sleep(200) // the HTTP round trip
    completed.incrementAndGet()
    return 16_000L + supplier * 10
}

fun main() {
    val completed = AtomicInteger()
    Executors.newVirtualThreadPerTaskExecutor().use { executor -> // close() waits for submitted tasks
        val calls = (1..40).map { supplier ->
            CompletableFuture.supplyAsync({ blockingQuote(supplier, completed) }, executor)
        }
        val import = CompletableFuture.allOf(*calls.toTypedArray())
        Thread.sleep(50)
        import.cancel(true) // the buying team cancels the import
        println("import cancelled: ${import.isCancelled}") // -> import cancelled: true
    }
    // Cancelling the combined future cancelled nothing behind it.
    println("supplier calls that ran anyway: ${completed.get()} of 40") // -> supplier calls that ran anyway: 40 of 40
}
