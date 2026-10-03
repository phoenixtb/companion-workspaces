package com.shelfwise.catalog.events

import com.shelfwise.events.ProductEvent
import com.shelfwise.events.ProductEvents
import com.shelfwise.events.type
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.takeWhile
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.KafkaException
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.event.TransactionalEventListener

// Turns committed changes into Kafka records and into a live stream for the SSE endpoint.
@Component
class ProductEventRelay(private val kafka: KafkaTemplate<String, ProductEvent>) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)

    // Hot and without replay: a dashboard sees changes from the moment it connects (Part 11).
    private val _changes = MutableSharedFlow<ProductEvent>(extraBufferCapacity = 256)
    private val shutdown = MutableSharedFlow<Unit>(replay = 1)
    val subscribers: StateFlow<Int> = _changes.subscriptionCount

    // Every stream ends when the application stops: stop() emits a null sentinel, takeWhile ends the
    // flow on it, and replay = 1 makes streams opened after stop() end at once. Without this, an
    // endless SSE response keeps Tomcat's graceful shutdown waiting for its full timeout.
    val changes: Flow<ProductEvent> = merge(_changes, shutdown.map { null }).takeWhile { it != null }.filterNotNull()

    // AFTER_COMMIT: a rolled-back transaction publishes nothing (the remaining gap is in the post).
    // Spring logs and swallows anything thrown here, so failures are caught and logged explicitly.
    @TransactionalEventListener
    fun relay(event: ProductEvent) {
        if (!_changes.tryEmit(event)) log.warn("A dashboard fell behind; dropped {} for {}", event.type, event.sku)
        val record = ProducerRecord(ProductEvents.TOPIC, event.sku, event) // key = SKU: per-product ordering
        record.headers().add("event-type", event.type.encodeToByteArray())
        try {
            kafka.send(record).whenComplete { _, failure ->
                if (failure != null) log.error("Could not publish {} for {}", event.type, event.sku, failure)
            }
        } catch (e: KafkaException) { // thrown synchronously, e.g. no broker metadata within max.block.ms
            log.error("Could not publish {} for {}", event.type, event.sku, e)
        }
    }

    // SmartLifecycle's default phase stops before the web server's graceful-shutdown phase.
    @Volatile
    private var running = false

    override fun start() {
        running = true
    }

    override fun stop() {
        shutdown.tryEmit(Unit)
        running = false
    }

    override fun isRunning() = running
}
