package com.shelfwise.assistant.lc4j

import com.shelfwise.assistant.ports.IndexedDocuments
import com.shelfwise.assistant.ports.ProductIndexer
import com.shelfwise.events.ProductSnapshot
import dev.langchain4j.data.document.Metadata
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.store.embedding.qdrant.QdrantEmbeddingStore
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("lc4j")
class Lc4jProductIndexer(
    private val embeddingModel: EmbeddingModel,
    private val store: QdrantEmbeddingStore,
) : ProductIndexer {
    override fun upsert(product: ProductSnapshot) =
        upsert(IndexedDocuments.productId(product.sku), IndexedDocuments.productText(product), lc4jMetadata(product))

    override fun delete(sku: String) = store.remove(IndexedDocuments.productId(sku))

    override fun upsertPolicy(name: String, text: String) =
        upsert(IndexedDocuments.policyId(name), text, mapOf("type" to "policy", "policy" to name))

    // addAll with explicit ids: the overload without ids (and EmbeddingStoreIngestor, which uses it)
    // generates random ones, and every redelivered event would add a duplicate point.
    private fun upsert(id: String, text: String, metadata: Map<String, Any>) {
        val segment = TextSegment.from(text, Metadata.from(metadata))
        store.addAll(listOf(id), listOf(embeddingModel.embed(segment).content()), listOf(segment))
    }

    // LangChain4j metadata holds strings and numbers, not lists: the tags become one string.
    private fun lc4jMetadata(product: ProductSnapshot) =
        IndexedDocuments.productMetadata(product) + ("dietaryTags" to product.dietaryTags.joinToString(","))
}
