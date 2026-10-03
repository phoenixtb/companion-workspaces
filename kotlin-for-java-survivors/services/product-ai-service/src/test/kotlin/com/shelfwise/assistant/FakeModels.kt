package com.shelfwise.assistant

import org.springframework.ai.chat.messages.AssistantMessage
import org.springframework.ai.chat.model.ChatModel
import org.springframework.ai.chat.model.ChatResponse
import org.springframework.ai.chat.model.Generation
import org.springframework.ai.chat.prompt.Prompt
import org.springframework.ai.document.Document
import org.springframework.ai.embedding.Embedding
import org.springframework.ai.embedding.EmbeddingModel
import org.springframework.ai.embedding.EmbeddingRequest
import org.springframework.ai.embedding.EmbeddingResponse
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import kotlin.math.sqrt

// Deterministic stand-ins for Ollama, so tests exercise the real Qdrant, Kafka and Spring AI wiring
// without a model. Embeddings are hashed bags of words: texts that share words are close.
class BagOfWordsEmbeddingModel : EmbeddingModel {
    override fun call(request: EmbeddingRequest) =
        EmbeddingResponse(request.instructions.mapIndexed { i, text -> Embedding(vectorOf(text), i) })

    override fun embed(document: Document) = vectorOf(document.text.orEmpty())

    override fun dimensions() = DIMENSIONS

    private fun vectorOf(text: String): FloatArray {
        val vector = FloatArray(DIMENSIONS)
        // Words of four letters or more, with a trailing "s" dropped: crude, but enough to make
        // "grinder" match "grinders" and to ignore "the", "can" and "who".
        Regex("[a-z0-9]{4,}").findAll(text.lowercase()).map { it.value.removeSuffix("s") }
            .forEach { vector[Math.floorMod(it.hashCode(), DIMENSIONS)] += 1f }
        val norm = sqrt(vector.sumOf { (it * it).toDouble() }).toFloat().takeIf { it > 0 } ?: 1f
        return FloatArray(DIMENSIONS) { vector[it] / norm }
    }

    companion object {
        const val DIMENSIONS = 512
    }
}

// What both adapters' fake chat models offer the shared tests: the text of the last prompt.
interface PromptRecorder {
    val lastPrompt: String?
}

// Records the prompt it was given and answers with a fixed sentence: enough to check what the
// RAG advisor put in front of the model.
class RecordingChatModel : ChatModel, PromptRecorder {
    override var lastPrompt: String? = null

    override fun call(prompt: Prompt): ChatResponse {
        lastPrompt = prompt.contents
        return ChatResponse(listOf(Generation(AssistantMessage("(fake model answer)"))))
    }
}

@TestConfiguration(proxyBeanMethods = false)
class FakeModels {
    @Bean
    fun embeddingModel() = BagOfWordsEmbeddingModel()

    @Bean
    fun chatModel() = RecordingChatModel()
}
