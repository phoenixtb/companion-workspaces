package com.shelfwise.assistant

import dev.langchain4j.data.message.UserMessage
import dev.langchain4j.exception.AsyncNotSupportedException
import dev.langchain4j.kotlin.model.chat.chatAsync
import dev.langchain4j.model.chat.request.ChatRequest
import dev.langchain4j.model.chat.response.ChatResponse
import dev.langchain4j.service.AiServices
import dev.langchain4j.service.IllegalConfigurationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

// Without an import, Result is kotlin.Result: a value class, erased to Object in the JVM signature.
interface ForgotTheImport {
    fun answer(question: String): Result<String>
}

class KotlinOnJavaLibrariesTest {
    private val model = Lc4jRecordingChatModel()
    private val request = ChatRequest.builder().messages(UserMessage.from("hello")).build()

    @Test
    fun `a member function beats an extension with the same parameters`() {
        // langchain4j-kotlin's suspend chatAsync(request) is shadowed by ChatModel's own chatAsync(request),
        // an @Experimental member whose future fails for models without a native async call (Ollama's too).
        val future = model.chatAsync(request)
        assertIs<CompletableFuture<*>>(future)
        assertIs<AsyncNotSupportedException>(assertFailsWith<ExecutionException> { future.get() }.cause)
        // Only an argument the member doesn't take selects the suspend extension.
        assertIs<ChatResponse>(runBlocking { model.chatAsync(request, Dispatchers.IO) })
    }

    @Test
    fun `kotlin Result is not LangChain4j's Result`() {
        val failure = assertFailsWith<IllegalConfigurationException> {
            AiServices.builder(ForgotTheImport::class.java).chatModel(model).build().answer("hi")
        }
        assertEquals("Illegal method return type: class java.lang.Object", failure.message)
    }
}
