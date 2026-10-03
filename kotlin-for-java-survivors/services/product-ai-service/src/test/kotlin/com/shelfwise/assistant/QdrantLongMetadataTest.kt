package com.shelfwise.assistant

import org.junit.jupiter.api.Test
import org.springframework.ai.document.Document
import org.springframework.ai.vectorstore.SearchRequest
import org.springframework.ai.vectorstore.VectorStore
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals

// Why IndexedDocuments stores pricePaise as an Int: a Long goes to Qdrant as a string.
@AssistantSpringTest
class QdrantLongMetadataTest(@Autowired private val vectorStore: VectorStore) {
    @Test
    fun `a Long in metadata comes back as a String and fails numeric filters`() {
        val id = UUID.randomUUID().toString()
        vectorStore.add(listOf(Document.builder().id(id).text("long metadata probe").metadata(mapOf("probe" to 42L)).build()))

        val stored = vectorStore.similaritySearch(SearchRequest.builder().query("long metadata probe").filterExpression("probe == '42'").build())
        assertEquals("42", stored.single().metadata["probe"]) // a String now

        val numeric = vectorStore.similaritySearch(SearchRequest.builder().query("long metadata probe").filterExpression("probe <= 100").build())
        assertEquals(0, numeric.size) // the range filter can't see it
        vectorStore.delete(listOf(id))
    }
}
