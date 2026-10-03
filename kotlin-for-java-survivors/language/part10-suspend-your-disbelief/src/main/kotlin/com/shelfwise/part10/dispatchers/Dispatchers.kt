package com.shelfwise.part10.dispatchers

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicInteger

fun threadName() = Thread.currentThread().name.substringBefore(" @") // drop the debug-mode coroutine suffix

fun main() = runBlocking {
    val caller = threadName() // 'main' when run directly; the test runner's thread under test

    // Default: CPU-bound work, one thread per core. IO: blocking calls, up to 64 threads by default.
    // They share one pool of worker threads; withContext switches and switches back.
    val onDefault = withContext(Dispatchers.Default) { threadName() }
    val onIo = withContext(Dispatchers.IO) { threadName() }
    println(onDefault.startsWith("DefaultDispatcher-worker")) // -> true
    println(onIo.startsWith("DefaultDispatcher-worker")) // -> true
    println(threadName() == caller) // -> true

    // limitedParallelism: a view of a dispatcher that runs at most N coroutines at a time.
    // Use it as a bulkhead, e.g. for a supplier API that allows two concurrent calls.
    val supplierApi = Dispatchers.IO.limitedParallelism(2)
    val inFlight = AtomicInteger()
    val maxInFlight = AtomicInteger()
    (1..10).map {
        async(supplierApi) {
            val now = inFlight.incrementAndGet()
            maxInFlight.accumulateAndGet(now, ::maxOf)
            Thread.sleep(20) // a blocking client call, fine on IO
            inFlight.decrementAndGet()
        }
    }.awaitAll()
    println("max concurrent calls: ${maxInFlight.get()}") // -> max concurrent calls: 2
}
