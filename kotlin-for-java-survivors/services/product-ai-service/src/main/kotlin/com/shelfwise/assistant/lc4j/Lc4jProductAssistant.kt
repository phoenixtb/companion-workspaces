package com.shelfwise.assistant.lc4j

import com.shelfwise.assistant.ports.Answer
import com.shelfwise.assistant.ports.AssistantPrompts
import com.shelfwise.assistant.ports.ProductAssistant
import com.shelfwise.assistant.ports.ProductHit
import com.shelfwise.assistant.tools.CatalogTools
import dev.langchain4j.data.message.UserMessage
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.rag.DefaultRetrievalAugmentor
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever
import dev.langchain4j.service.AiServices
import dev.langchain4j.service.Result
import dev.langchain4j.service.SystemMessage
import dev.langchain4j.service.tool.ToolArgumentsErrorHandler
import dev.langchain4j.service.tool.ToolExecutionErrorHandler
import dev.langchain4j.store.embedding.EmbeddingSearchRequest
import dev.langchain4j.store.embedding.RelevanceScore
import dev.langchain4j.store.embedding.filter.Filter
import dev.langchain4j.store.embedding.filter.MetadataFilterBuilder.metadataKey
import dev.langchain4j.store.embedding.qdrant.QdrantEmbeddingStore
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

// LangChain4j's model of an assistant: an interface, implemented at runtime by AiServices.
// Result<String> carries the answer and the retrieved sources. @SystemMessage needs a constant,
// which is why AssistantPrompts.SYSTEM is a const val (and keeps its indentation).
interface ShopAssistant {
    @SystemMessage(AssistantPrompts.SYSTEM)
    fun answer(question: String): Result<String>
}

@Component
@Profile("lc4j")
class Lc4jProductAssistant(
    private val embeddingModel: EmbeddingModel,
    private val store: QdrantEmbeddingStore,
    chatModel: ChatModel,
    catalogTools: CatalogTools,
    @Value("\${assistant.rag.similarity-threshold}") similarityThreshold: Double,
) : ProductAssistant {
    // LangChain4j scores are "relevance", (cosine + 1) / 2, where Spring AI's are cosine similarity.
    // The configured threshold is a cosine, so it's converted: 0.6 cosine is 0.8 relevance.
    private val retriever = EmbeddingStoreContentRetriever.builder()
        .embeddingStore(store)
        .embeddingModel(embeddingModel)
        .minScore(RelevanceScore.fromCosineSimilarity(similarityThreshold))
        .maxResults(4)
        .build()

    private val assistant: ShopAssistant = AiServices.builder(ShopAssistant::class.java)
        .chatModel(chatModel)
        .retrievalAugmentor(
            DefaultRetrievalAugmentor.builder()
                .contentRetriever(retriever)
                // LangChain4j's default injector passes the question through untouched when nothing was
                // retrieved, which lets the model improvise. Same prompts as the Spring AI adapter instead.
                .contentInjector { contents, message ->
                    val question = (message as UserMessage).singleText()
                    UserMessage.from(
                        if (contents.isEmpty()) {
                            AssistantPrompts.withoutContext(question)
                        } else {
                            AssistantPrompts.WITH_CONTEXT.trimIndent()
                                .replace("{context}", contents.joinToString("\n\n") { it.textSegment().text() })
                                .replace("{query}", question)
                        },
                    )
                }
                .build(),
        )
        .tools(catalogTools)
        // LangChain4j's recommended settings: bad arguments go back to the model so it can retry, but a
        // tool's exception fails the call unless it was written for the model (ToolErrorVisibleToLlm),
        // so internal URLs and SQL never reach the prompt.
        .toolArgumentsErrorHandler(ToolArgumentsErrorHandler.sendExceptionMessageToLlm())
        .toolExecutionErrorHandler(ToolExecutionErrorHandler.failInvocationUnlessVisibleToLlm())
        .build()

    override fun search(query: String, maxPricePaise: Long?, dietaryTag: String?, limit: Int): List<ProductHit> {
        val filter = listOfNotNull(
            metadataKey("type").isEqualTo("product"),
            maxPricePaise?.let { metadataKey("pricePaise").isLessThanOrEqualTo(it) },
            dietaryTag?.let { metadataKey("dietaryTags").containsString(it) }, // a substring of "gluten-free,vegan"
        ).reduce(Filter::and)
        val request = EmbeddingSearchRequest.builder()
            .queryEmbedding(embeddingModel.embed(query).content())
            .maxResults(limit)
            .filter(filter)
            .build()
        return store.search(request).matches().map { match ->
            val metadata = match.embedded().metadata()
            ProductHit(
                sku = metadata.getString("sku")!!,
                name = metadata.getString("name")!!,
                pricePaise = metadata.getLong("pricePaise")!!, // stored as an Int; getLong accepts any number
                score = 2 * match.score() - 1, // back to cosine, as the Spring AI adapter reports it
            )
        }
    }

    override fun ask(question: String): Answer {
        val result = assistant.answer(question)
        return Answer(
            text = result.content(),
            sources = result.sources().map { it.textSegment().metadata().let { m -> m.getString("sku") ?: m.getString("policy")!! } }.distinct(),
        )
    }
}
