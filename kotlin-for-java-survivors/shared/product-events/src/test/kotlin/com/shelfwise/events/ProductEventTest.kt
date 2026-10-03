package com.shelfwise.events

import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.time.Instant
import kotlin.uuid.Uuid

class ProductEventTest {
    private val penne = ProductSnapshot(
        sku = "SHW-1001",
        name = "Durum Wheat Penne 500 g",
        description = "Made from hard durum wheat semolina.",
        category = "PASTA_AND_GRAINS",
        pricePaise = 16_500,
        supplier = "Banyan Agro Supplies",
    )
    private val created = ProductCreated(
        eventId = Uuid.parse("0199a4c2-7f00-7000-8000-000000000001"),
        sku = "SHW-1001",
        occurredAt = Instant.parse("2026-10-05T04:30:00Z"),
        product = penne,
    )

    @Test
    fun `events are JSON with a type discriminator`() {
        assertEquals(
            """{"type":"product-created","eventId":"0199a4c2-7f00-7000-8000-000000000001","sku":"SHW-1001",""" +
                """"occurredAt":"2026-10-05T04:30:00Z","product":{"sku":"SHW-1001","name":"Durum Wheat Penne 500 g",""" +
                """"description":"Made from hard durum wheat semolina.","category":"PASTA_AND_GRAINS","pricePaise":16500,""" +
                """"supplier":"Banyan Agro Supplies","dietaryTags":[]}}""",
            ProductEvents.encode(created).decodeToString(),
        )
    }

    @Test
    fun `a round trip through the Kafka serdes keeps the subtype`() {
        val bytes = ProductEventSerializer().serialize(ProductEvents.TOPIC, created)
        assertEquals(created, ProductEventDeserializer().deserialize(ProductEvents.TOPIC, bytes))
    }

    @Test
    fun `unknown properties from a newer producer are ignored`() {
        val newer = """{"type":"product-deleted","eventId":"0199a4c2-7f00-7000-8000-000000000002","sku":"SHW-1001",""" +
            """"occurredAt":"2026-10-05T04:31:00Z","reason":"discontinued by supplier"}"""
        assertEquals("SHW-1001", ProductEvents.decode(newer.encodeToByteArray()).sku)
    }

    @Test
    fun `the type property matches the serial name of every subtype`() {
        val events = listOf(
            created,
            ProductUpdated(created.eventId, created.sku, created.occurredAt, penne),
            ProductDeleted(created.eventId, created.sku, created.occurredAt),
        )
        for (event in events) {
            val json = ProductEvents.json.encodeToString(ProductEvent.serializer(), event)
            assertEquals(event.type, ProductEvents.json.parseToJsonElement(json).jsonObject["type"]!!.jsonPrimitive.content)
        }
    }
}
