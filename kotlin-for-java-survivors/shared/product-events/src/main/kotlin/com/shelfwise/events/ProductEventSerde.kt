package com.shelfwise.events

import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

object ProductEvents {
    const val TOPIC = "product-events"

    // "type" carries the @SerialName; unknown properties are ignored so producers can add fields
    // without breaking consumers that haven't upgraded yet; defaults are written out, so an empty
    // dietaryTags is [] on the wire rather than missing (kotlinx.serialization omits them by default).
    val json = Json {
        classDiscriminator = "type"
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    fun encode(event: ProductEvent): ByteArray = json.encodeToString(ProductEvent.serializer(), event).encodeToByteArray()

    fun decode(bytes: ByteArray): ProductEvent = json.decodeFromString(ProductEvent.serializer(), bytes.decodeToString())
}

// Kafka serdes, referenced by class name in the services' configuration.
class ProductEventSerializer : Serializer<ProductEvent> {
    override fun serialize(topic: String, data: ProductEvent?): ByteArray? = data?.let(ProductEvents::encode)
}

class ProductEventDeserializer : Deserializer<ProductEvent> {
    override fun deserialize(topic: String, data: ByteArray?): ProductEvent? = data?.let(ProductEvents::decode)
}
