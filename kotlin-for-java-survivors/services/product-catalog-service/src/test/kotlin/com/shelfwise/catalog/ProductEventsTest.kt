package com.shelfwise.catalog

import com.shelfwise.catalog.product.ProductService
import com.shelfwise.events.ProductCreated
import com.shelfwise.events.ProductDeleted
import com.shelfwise.events.ProductEvent
import com.shelfwise.events.ProductEventDeserializer
import com.shelfwise.events.ProductEvents
import com.shelfwise.events.ProductUpdated
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.io.IOException
import java.io.StringReader
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

@SpringBootTest
@Import(TestcontainersConfiguration::class)
class ProductEventsTest(
    @Autowired private val catalog: ProductService,
    @Autowired private val kafka: KafkaConnectionDetails,
) {
    private fun consumer() = KafkaConsumer(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.consumer.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "events-test-${System.nanoTime()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ),
        StringDeserializer(),
        ProductEventDeserializer(),
    ).apply { subscribe(listOf(ProductEvents.TOPIC)) }

    // Everything published for one SKU, in order: the key keeps a product's events on one partition.
    private fun eventsFor(sku: String, expected: Int): List<Pair<String, ProductEvent>> = consumer().use { consumer ->
        val seen = mutableListOf<Pair<String, ProductEvent>>()
        val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos()
        while (seen.size < expected && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200)).filter { it.key() == sku }.forEach { record ->
                seen += String(record.headers().lastHeader("event-type").value()) to record.value()
            }
        }
        seen
    }

    @Test
    fun `create, reprice and delete each publish one event, after commit`() {
        catalog.create(newProduct("SHW-1401"))
        // A rolled-back import publishes nothing, although it repriced SHW-1401 before failing.
        assertFailsWith<IOException> { catalog.importPriceList(FailingAfter("SHW-1401,9900\n")) }
        catalog.reprice("SHW-1401", 11_000)
        catalog.delete("SHW-1401")

        val events = eventsFor("SHW-1401", expected = 3)

        assertEquals(listOf("product-created", "product-updated", "product-deleted"), events.map { it.first })
        assertIs<ProductCreated>(events[0].second)
        assertEquals(11_000, assertIs<ProductUpdated>(events[1].second).product.pricePaise)
        assertIs<ProductDeleted>(events[2].second)
    }
}
