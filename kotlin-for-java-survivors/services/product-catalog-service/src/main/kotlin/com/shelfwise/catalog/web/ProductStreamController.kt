package com.shelfwise.catalog.web

import com.shelfwise.catalog.events.ProductEventRelay
import com.shelfwise.catalog.quotes.BestQuote
import com.shelfwise.catalog.quotes.SupplierQuotes
import com.shelfwise.events.ProductEvent
import com.shelfwise.events.ProductEvents
import com.shelfwise.events.type
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

// Spring MVC runs suspend functions and Flow return values by adapting them to Reactor types.
@RestController
class ProductStreamController(
    private val relay: ProductEventRelay,
    private val quotes: SupplierQuotes,
) {
    // Server-sent events: one per committed change, for as long as the client stays connected.
    @GetMapping("/api/products/changes", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun changes(): Flow<ServerSentEvent<String>> = relay.changes.map { event ->
        ServerSentEvent.builder(ProductEvents.json.encodeToString(ProductEvent.serializer(), event))
            .event(event.type)
            .id(event.eventId.toString())
            .build()
    }

    @GetMapping("/api/products/{sku}/best-quote")
    suspend fun bestQuote(@PathVariable sku: String): BestQuote = quotes.bestQuote(sku)
}
