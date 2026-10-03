package com.shelfwise.events

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Instant
import kotlin.uuid.Uuid

// Every change to a product, as an event. Sealed, so a consumer's when is exhaustive and a new
// event type is a compile error in every consumer until it's handled.
@Serializable
sealed interface ProductEvent {
    val eventId: Uuid
    val sku: String
    val occurredAt: Instant
}

@Serializable
@SerialName("product-created")
data class ProductCreated(
    override val eventId: Uuid,
    override val sku: String,
    override val occurredAt: Instant,
    val product: ProductSnapshot,
) : ProductEvent

@Serializable
@SerialName("product-updated")
data class ProductUpdated(
    override val eventId: Uuid,
    override val sku: String,
    override val occurredAt: Instant,
    val product: ProductSnapshot,
) : ProductEvent

@Serializable
@SerialName("product-deleted")
data class ProductDeleted(
    override val eventId: Uuid,
    override val sku: String,
    override val occurredAt: Instant,
) : ProductEvent

// The full state after the change, so a consumer never has to call back for details.
@Serializable
data class ProductSnapshot(
    val sku: String,
    val name: String,
    val description: String,
    val category: String,
    val pricePaise: Long,
    val supplier: String,
    val dietaryTags: List<String> = emptyList(),
)

// The same names as the JSON "type", for Kafka headers and SSE event names. Exhaustive: a new
// subtype doesn't compile until it has a name here.
val ProductEvent.type: String
    get() = when (this) {
        is ProductCreated -> "product-created"
        is ProductUpdated -> "product-updated"
        is ProductDeleted -> "product-deleted"
    }
