package com.shelfwise.assistant.ports

import com.shelfwise.events.ProductSnapshot
import java.util.UUID

// How products and policies become documents, shared by both adapters so they index the same text.
object IndexedDocuments {
    // Deterministic point ids: the same SKU always maps to the same point, so a redelivered or
    // replayed event overwrites instead of duplicating. That's the idempotence Kafka needs.
    fun productId(sku: String): String = UUID.nameUUIDFromBytes("product:$sku".encodeToByteArray()).toString()

    fun policyId(name: String): String = UUID.nameUUIDFromBytes("policy:$name".encodeToByteArray()).toString()

    fun productText(p: ProductSnapshot): String = buildString {
        appendLine("${p.name} (SKU ${p.sku})")
        if (p.description.isNotBlank()) appendLine(p.description)
        append("Category: ${p.category.lowercase().replace('_', ' ')}. ")
        append("Price: ₹%d.%02d.".format(p.pricePaise / 100, p.pricePaise % 100))
        if (p.dietaryTags.isNotEmpty()) append(" Dietary: ${p.dietaryTags.joinToString()}.")
    }

    fun productMetadata(p: ProductSnapshot): Map<String, Any> = mapOf(
        "type" to "product",
        "sku" to p.sku,
        "name" to p.name,
        "category" to p.category,
        // An Int, not a Long: Spring AI's Qdrant store writes Long values as strings, and a numeric
        // range filter on a string field matches nothing. Paise fit in an Int up to ₹2 crore.
        "pricePaise" to Math.toIntExact(p.pricePaise),
        "dietaryTags" to p.dietaryTags,
    )
}
