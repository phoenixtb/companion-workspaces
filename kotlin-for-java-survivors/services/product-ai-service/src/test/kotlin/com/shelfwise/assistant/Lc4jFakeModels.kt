package com.shelfwise.assistant

import dev.langchain4j.data.embedding.Embedding
import dev.langchain4j.data.message.AiMessage
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.model.chat.request.ChatRequest
import dev.langchain4j.model.chat.response.ChatResponse
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.model.output.Response
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile

// The same stand-ins as FakeModels, behind LangChain4j's interfaces: the same vectors for the same
// text, so both adapters retrieve the same documents.
class Lc4jBagOfWordsEmbeddingModel(private val vectors: BagOfWordsEmbeddingModel = BagOfWordsEmbeddingModel()) : EmbeddingModel {
    override fun embedAll(textSegments: List<TextSegment>): Response<List<Embedding>> =
        Response.from(textSegments.map { Embedding.from(vectors.embed(it.text())) })

    override fun dimension() = BagOfWordsEmbeddingModel.DIMENSIONS
}

class Lc4jRecordingChatModel : ChatModel, PromptRecorder {
    override var lastPrompt: String? = null

    override fun doChat(chatRequest: ChatRequest): ChatResponse {
        lastPrompt = chatRequest.messages().joinToString("\n") { it.toString() }
        return ChatResponse.builder().aiMessage(AiMessage.from("(fake model answer)")).build()
    }
}

@TestConfiguration(proxyBeanMethods = false)
@Profile("lc4j")
class Lc4jFakeModels {
    // @Primary: application-lc4j.yaml also makes the starter create real Ollama models.
    @Bean
    @Primary
    fun lc4jEmbeddingModel() = Lc4jBagOfWordsEmbeddingModel()

    @Bean
    @Primary
    fun lc4jChatModel() = Lc4jRecordingChatModel()
}
