package com.shelfwise.assistant

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import

// One composed annotation for every Spring test in this service: identical configuration means one
// cached context, so Kafka and Qdrant start once for the whole suite.
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@SpringBootTest(
    properties = [
        "spring.ai.model.chat=none", // no Ollama in tests: FakeModels provides both models
        "spring.ai.model.embedding=none",
        "assistant.rag.similarity-threshold=0.25", // bag-of-words similarities run lower than a real model's
    ],
)
@Import(TestcontainersConfiguration::class, FakeModels::class, Lc4jFakeModels::class)
annotation class AssistantSpringTest
