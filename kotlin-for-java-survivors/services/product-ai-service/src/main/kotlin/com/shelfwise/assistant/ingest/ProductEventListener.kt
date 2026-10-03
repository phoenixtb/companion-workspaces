package com.shelfwise.assistant.ingest

import com.shelfwise.assistant.ports.ProductIndexer
import com.shelfwise.events.ProductCreated
import com.shelfwise.events.ProductDeleted
import com.shelfwise.events.ProductEvent
import com.shelfwise.events.ProductEvents
import com.shelfwise.events.ProductUpdated
import com.shelfwise.events.type
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

// Keeps the vector index in step with the catalog. Exhaustive over the sealed hierarchy: a new event
// type in shared/product-events doesn't compile here until it's handled.
@Component
class ProductEventListener(private val indexer: ProductIndexer) {
    private val log = LoggerFactory.getLogger(javaClass)

    // A plain (blocking) listener: indexing calls the embedding model and Qdrant synchronously. With the
    // default AckMode.BATCH, offsets are committed once every record from a poll has been processed.
    @KafkaListener(topics = [ProductEvents.TOPIC], groupId = "\${assistant.consumer-group}")
    fun on(event: ProductEvent) {
        when (event) {
            is ProductCreated -> indexer.upsert(event.product)
            is ProductUpdated -> indexer.upsert(event.product)
            is ProductDeleted -> indexer.delete(event.sku)
        }
        log.info("Indexed {} for {}", event.type, event.sku)
    }
}
