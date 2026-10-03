package com.shelfwise.part11.threads

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors

// A blocking SDK call that can't be rewritten: it holds its thread for the whole call.
fun blockingSupplierCall(i: Int): Int {
    Thread.sleep(100)
    return i
}

// The two models combine: coroutines for structure, a virtual-thread dispatcher for blocking calls.
fun main() = runBlocking {
    Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher().use { virtualThreads ->
        val results = withContext(virtualThreads) {
            (1..200).map { i -> async { blockingSupplierCall(i) } }.awaitAll()
        }
        println("${results.size} blocking calls, sum ${results.sum()}") // -> 200 blocking calls, sum 20100
        println(withContext(virtualThreads) { Thread.currentThread().isVirtual }) // -> true
    }
}
