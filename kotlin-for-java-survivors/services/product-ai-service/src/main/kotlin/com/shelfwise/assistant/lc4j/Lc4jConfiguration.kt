package com.shelfwise.assistant.lc4j

import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.store.embedding.qdrant.QdrantEmbeddingStore
import io.qdrant.client.QdrantClient
import io.qdrant.client.QdrantGrpcClient
import io.qdrant.client.grpc.Collections.Distance
import io.qdrant.client.grpc.Collections.VectorParams
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

// The LangChain4j adapter's infrastructure. The chat and embedding models come from
// langchain4j-ollama-spring-boot4-starter (application-lc4j.yaml); Qdrant is wired here, because
// there is no Boot 4 starter for LangChain4j's Qdrant store.
@Configuration(proxyBeanMethods = false)
@Profile("lc4j")
class Lc4jConfiguration {
    @Bean(destroyMethod = "close")
    fun qdrantClient(
        @Value("\${assistant.qdrant.host}") host: String,
        @Value("\${assistant.qdrant.port}") port: Int,
    ) = QdrantClient(QdrantGrpcClient.newBuilder(host, port, false).build())

    // Unlike Spring AI's initialize-schema, LangChain4j doesn't create collections: do it once, sized
    // to the embedding model (one probe embedding tells us the dimensions).
    @Bean
    fun embeddingStore(
        client: QdrantClient,
        embeddingModel: EmbeddingModel,
        @Value("\${assistant.lc4j.collection}") collection: String,
    ): QdrantEmbeddingStore {
        if (!client.collectionExistsAsync(collection).get()) {
            val dimensions = embeddingModel.dimension()
            client.createCollectionAsync(
                collection,
                VectorParams.newBuilder().setSize(dimensions.toLong()).setDistance(Distance.Cosine).build(),
            ).get()
        }
        return QdrantEmbeddingStore(client, collection, "text_segment")
    }
}
