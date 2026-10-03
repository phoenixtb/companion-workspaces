package com.shelfwise.assistant.ports

import com.shelfwise.events.ProductSnapshot

// What the service needs from an AI library, in its own words. Part 15 implements both ports with
// Spring AI; Part 16 implements them again with LangChain4j, behind a profile.
interface ProductIndexer {
    fun upsert(product: ProductSnapshot)

    fun delete(sku: String)

    fun upsertPolicy(name: String, text: String)
}

data class ProductHit(val sku: String, val name: String, val pricePaise: Long, val score: Double)

data class Answer(val text: String, val sources: List<String>)

interface ProductAssistant {
    fun search(query: String, maxPricePaise: Long? = null, dietaryTag: String? = null, limit: Int = 5): List<ProductHit>

    fun ask(question: String): Answer
}
