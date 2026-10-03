package com.shelfwise.part12.api

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture

// A suspend API with a Java-friendly twin. The class owns the scope its futures run in,
// and closing it cancels whatever is still running.
class QuoteService : AutoCloseable {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    suspend fun bestQuote(sku: String): Long {
        delay(10) // a supplier call
        return if (sku == "SHW-1001") 16_000 else 70_000
    }

    fun bestQuoteAsync(sku: String): CompletableFuture<Long> = scope.future { bestQuote(sku) }

    override fun close() = scope.cancel()
}
