package com.shelfwise.assistant

import com.shelfwise.assistant.ports.ProductAssistant
import com.shelfwise.events.ProductCreated
import com.shelfwise.events.ProductDeleted
import com.shelfwise.events.ProductEvent
import com.shelfwise.events.ProductEventSerializer
import com.shelfwise.events.ProductEvents
import com.shelfwise.events.ProductSnapshot
import com.shelfwise.events.ProductUpdated
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.uuid.Uuid

@AssistantSpringTest
class AssistantTest(
    @Autowired private val assistant: ProductAssistant,
    @Autowired private val chatModel: RecordingChatModel,
    @Autowired private val kafka: KafkaConnectionDetails,
) {
    private fun publish(vararg events: ProductEvent) =
        KafkaProducer(mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.producer.bootstrapServers), StringSerializer(), ProductEventSerializer())
            .use { producer -> events.forEach { producer.send(ProducerRecord(ProductEvents.TOPIC, it.sku, it)) } }

    private fun pasta(sku: String, name: String, paise: Long, vararg tags: String) =
        ProductSnapshot(sku, name, "Pasta made from $name.", "PASTA_AND_GRAINS", paise, "Valley Fresh Producers", tags.toList())

    private fun created(p: ProductSnapshot) = ProductCreated(Uuid.random(), p.sku, Clock.System.now(), p)

    private fun eventually(check: () -> Boolean) {
        val deadline = System.nanoTime() + 20_000_000_000
        while (!check()) {
            check(System.nanoTime() < deadline) { "condition not met within 20 s" }
            Thread.sleep(200)
        }
    }

    @Test
    fun `product events become searchable, with filters`() {
        publish(
            created(pasta("SHW-1003", "millet pasta gluten free", 19_500, "gluten-free", "vegan")),
            created(pasta("SHW-1004", "quinoa pasta gluten free", 25_000, "gluten-free")),
            created(pasta("SHW-1001", "durum wheat penne pasta", 16_500)),
        )

        eventually { assistant.search("gluten free pasta").size >= 3 }

        val hits = assistant.search("gluten free pasta", maxPricePaise = 20_000, dietaryTag = "gluten-free")
        assertEquals(listOf("SHW-1003"), hits.map { it.sku })
    }

    @Test
    fun `replayed and updated events overwrite, and deletes remove`() {
        val kettle = ProductSnapshot("SHW-1014", "Electric Kettle 1.5L", "Auto shut-off kettle.", "SMALL_APPLIANCES", 189_500, "Himalayan Artisan Products")
        publish(created(kettle), created(kettle)) // the same event twice, as after a redelivery
        publish(ProductUpdated(Uuid.random(), kettle.sku, Clock.System.now(), kettle.copy(pricePaise = 179_500)))

        eventually { assistant.search("electric kettle").singleOrNull { it.sku == "SHW-1014" }?.pricePaise == 179_500L }
        assertEquals(1, assistant.search("electric kettle", limit = 10).count { it.sku == "SHW-1014" })

        publish(ProductDeleted(Uuid.random(), kettle.sku, Clock.System.now()))
        eventually { assistant.search("electric kettle", limit = 10).none { it.sku == "SHW-1014" } }
    }

    @Test
    fun `an unreadable record is skipped, not retried forever`() {
        KafkaProducer(mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.producer.bootstrapServers), StringSerializer(), ByteArraySerializer())
            .use { it.send(ProducerRecord(ProductEvents.TOPIC, "SHW-1999", """{"type":"product-archived","sku":"SHW-1999"}""".encodeToByteArray())) }
        publish(created(pasta("SHW-1009", "fusilli pasta tricolour", 14_000)))

        eventually { assistant.search("fusilli tricolour").any { it.sku == "SHW-1009" } } // the record after it got through
    }

    @Test
    fun `answers are grounded in retrieved policies`() {
        val answer = assistant.ask("Can I return a mixer grinder that stopped working?")

        assertTrue("returns-small-appliances" in answer.sources)
        val prompt = chatModel.lastPrompt!!.contents
        assertTrue("There is no \"no questions asked\" return policy." in prompt) // the policy text reached the model
    }

    @Test
    fun `with nothing relevant retrieved, the model is told to decline`() {
        val answer = assistant.ask("Who won the cricket match yesterday?")

        assertEquals(emptyList(), answer.sources)
        val prompt = chatModel.lastPrompt!!.contents
        assertTrue("No store documents match this query." in prompt)
        assertTrue("Query: Who won the cricket match yesterday?" in prompt) // the question survives, for tools
    }
}
