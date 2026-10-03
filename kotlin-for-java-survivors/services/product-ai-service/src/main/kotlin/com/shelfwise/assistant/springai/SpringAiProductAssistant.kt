package com.shelfwise.assistant.springai

import com.shelfwise.assistant.ports.Answer
import com.shelfwise.assistant.ports.AssistantPrompts
import com.shelfwise.assistant.ports.ProductAssistant
import com.shelfwise.assistant.ports.ProductHit
import com.shelfwise.assistant.tools.CatalogTools
import org.springframework.ai.chat.client.ChatClient
import org.springframework.ai.chat.prompt.PromptTemplate
import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query
import org.springframework.ai.rag.advisor.RetrievalAugmentationAdvisor
import org.springframework.ai.rag.generation.augmentation.ContextualQueryAugmenter
import org.springframework.ai.rag.retrieval.search.VectorStoreDocumentRetriever
import org.springframework.ai.vectorstore.SearchRequest
import org.springframework.ai.vectorstore.VectorStore
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("!lc4j")
class SpringAiProductAssistant(
    private val vectorStore: VectorStore,
    chatClient: ChatClient.Builder,
    catalogTools: CatalogTools,
    @Value("\${assistant.rag.similarity-threshold}") similarityThreshold: Double,
) : ProductAssistant {
    private val withContext = ContextualQueryAugmenter.builder()
        .promptTemplate(PromptTemplate(AssistantPrompts.WITH_CONTEXT.trimIndent()))
        .build()

    private val chat = chatClient
        .defaultSystem(AssistantPrompts.SYSTEM.trimIndent())
        .defaultAdvisors(
            RetrievalAugmentationAdvisor.builder()
                .documentRetriever(
                    VectorStoreDocumentRetriever.builder()
                        .vectorStore(vectorStore)
                        .similarityThreshold(similarityThreshold) // weak matches are worse than none
                        .topK(4)
                        .build(),
                )
                // With nothing relevant retrieved, the model must not improvise a return policy, but a
                // tool may still answer. ContextualQueryAugmenter's own empty-context prompt drops the
                // user's question, so the empty case is handled here: a Kotlin lambda for a Java interface.
                .queryAugmenter { query, documents ->
                    if (documents.isEmpty()) Query(AssistantPrompts.withoutContext(query.text())) else withContext.augment(query, documents)
                }
                .build(),
        )
        .defaultTools(catalogTools)
        .build()

    override fun search(query: String, maxPricePaise: Long?, dietaryTag: String?, limit: Int): List<ProductHit> {
        val filter = FilterExpressionBuilder().run {
            listOfNotNull(
                eq("type", "product"),
                maxPricePaise?.let { lte("pricePaise", it) },
                dietaryTag?.let { eq("dietaryTags", it) }, // matches if any element of the list equals it
            ).reduce { all, condition -> and(all, condition) }.build()
        }
        val request = SearchRequest.builder().query(query).topK(limit).filterExpression(filter).build()
        return vectorStore.similaritySearch(request).map { doc ->
            ProductHit(
                sku = doc.metadata["sku"] as String,
                name = doc.metadata["name"] as String,
                pricePaise = (doc.metadata["pricePaise"] as Number).toLong(),
                score = doc.score ?: 0.0,
            )
        }
    }

    override fun ask(question: String): Answer {
        val response = chat.prompt().user(question).call().chatClientResponse()
        @Suppress("UNCHECKED_CAST")
        val sources = response.context()[RetrievalAugmentationAdvisor.DOCUMENT_CONTEXT] as List<Document>? ?: emptyList()
        return Answer(
            text = response.chatResponse()?.result?.output?.text.orEmpty(),
            sources = sources.map { it.metadata["sku"] as String? ?: it.metadata["policy"] as String }.distinct(),
        )
    }
}
