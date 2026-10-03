package com.shelfwise.assistant.springai

import com.shelfwise.assistant.ports.IndexedDocuments
import com.shelfwise.assistant.ports.ProductIndexer
import com.shelfwise.events.ProductSnapshot
import org.springframework.ai.document.Document
import org.springframework.ai.vectorstore.VectorStore
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("!lc4j")
class SpringAiProductIndexer(private val vectorStore: VectorStore) : ProductIndexer {
    // add() embeds the text with the EmbeddingModel and upserts the point: same id, new vector and payload.
    override fun upsert(product: ProductSnapshot) = vectorStore.add(
        listOf(
            Document.builder()
                .id(IndexedDocuments.productId(product.sku))
                .text(IndexedDocuments.productText(product))
                .metadata(IndexedDocuments.productMetadata(product))
                .build(),
        ),
    )

    override fun delete(sku: String) = vectorStore.delete(listOf(IndexedDocuments.productId(sku)))

    override fun upsertPolicy(name: String, text: String) = vectorStore.add(
        listOf(
            Document.builder()
                .id(IndexedDocuments.policyId(name))
                .text(text)
                .metadata(mapOf("type" to "policy", "policy" to name))
                .build(),
        ),
    )
}
