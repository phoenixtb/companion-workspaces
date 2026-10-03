package com.shelfwise.catalog.quotes

import com.shelfwise.catalog.product.SupplierRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.reactive.function.client.WebClientException
import org.springframework.web.service.annotation.GetExchange
import kotlin.time.Duration.Companion.milliseconds

data class SupplierQuote(val supplier: String, val sku: String, val pricePaise: Long)

// Part 10's supplier client, now a real HTTP client: an interface, and Spring generates the rest.
// A suspend function needs the WebClient-backed proxy (see IntegrationConfiguration).
interface SupplierQuoteClient {
    @GetExchange("/suppliers/{code}/quotes/{sku}")
    suspend fun quote(@PathVariable code: String, @PathVariable sku: String): SupplierQuote
}

data class BestQuote(val sku: String, val best: SupplierQuote?, val answered: Int, val asked: Int)

@Service
class SupplierQuotes(
    private val client: SupplierQuoteClient,
    private val suppliers: SupplierRepository,
) {
    suspend fun bestQuote(sku: String): BestQuote {
        // JPA blocks, so it runs on IO rather than on whatever thread resumed this coroutine.
        val codes = withContext(Dispatchers.IO) { suppliers.findAll().map { it.code } }
        val quotes = coroutineScope {
            codes.map { code -> async { quoteOrNull(code, sku) } }.awaitAll().filterNotNull()
        }
        return BestQuote(sku, quotes.minByOrNull { it.pricePaise }, answered = quotes.size, asked = codes.size)
    }

    // One supplier's answer, or null if it is slow or failing. Only HTTP failures are caught:
    // cancellation must keep propagating (Part 10).
    private suspend fun quoteOrNull(code: String, sku: String): SupplierQuote? =
        try {
            withTimeoutOrNull(500.milliseconds) { client.quote(code, sku) }
        } catch (e: WebClientException) {
            null
        }
}
