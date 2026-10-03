package com.shelfwise.part10.quotes

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

// Step 1: a supplier API client with a suspending call. In Part 14 this becomes a real HTTP client.
data class Quote(val supplier: String, val sku: String, val paise: Long)

class SupplierClient(
    val name: String,
    private val latency: Duration,
    private val failing: Boolean = false,
    private val completedCalls: AtomicInteger,
) {
    suspend fun quote(sku: String): Quote {
        delay(latency) // the round trip: suspends, holds no thread
        if (failing) throw IOException("$name: HTTP 503")
        completedCalls.incrementAndGet()
        return Quote(name, sku, 16_000 + latency.inWholeMilliseconds)
    }
}

// Step 2: one supplier, with its own timeout. A slow or failing supplier yields null, not an exception.
suspend fun SupplierClient.quoteOrNull(sku: String, timeout: Duration): Quote? =
    try {
        withTimeoutOrNull(timeout) { quote(sku) }
    } catch (e: IOException) {
        null
    }

// Step 3: fan out inside a scope. Every call is a child of this function: if the caller is
// cancelled, all of them are; the function can't return while any of them is still running.
suspend fun bestQuote(sku: String, suppliers: List<SupplierClient>, maxInFlight: Int): Quote? = coroutineScope {
    val permits = Semaphore(maxInFlight) // a bulkhead: at most maxInFlight calls at once
    suppliers
        .map { supplier -> async { permits.withPermit { supplier.quoteOrNull(sku, 300.milliseconds) } } }
        .awaitAll()
        .filterNotNull()
        .minByOrNull { it.paise }
}

// Step 4: forty suppliers, two failing and one too slow, on the caller's single thread.
fun main() = runBlocking {
    val completed = AtomicInteger()
    val suppliers = (0 until 40).map { i ->
        when (i) {
            7, 23 -> SupplierClient("supplier-$i", 80.milliseconds, failing = true, completedCalls = completed)
            31 -> SupplierClient("supplier-$i", 2_000.milliseconds, completedCalls = completed)
            else -> SupplierClient("supplier-$i", (50 + i * 5).milliseconds, completedCalls = completed)
        }
    }

    val best = bestQuote("SHW-1001", suppliers, maxInFlight = 10)
    println("best: ${best?.supplier} at ${best?.paise}") // -> best: supplier-0 at 16050
    println("answered: ${completed.get()} of 40") // -> answered: 37 of 40

    // Step 5: cancellation reaches every call in flight.
    completed.set(0)
    val import = launch { bestQuote("SHW-2040", suppliers, maxInFlight = 40) }
    delay(30)
    import.cancel()
    import.join()
    println("after cancel: ${completed.get()} calls completed, import cancelled: ${import.isCancelled}") // -> after cancel: 0 calls completed, import cancelled: true
}
