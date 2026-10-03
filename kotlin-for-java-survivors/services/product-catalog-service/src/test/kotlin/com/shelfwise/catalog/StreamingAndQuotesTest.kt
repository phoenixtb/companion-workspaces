package com.shelfwise.catalog

import com.shelfwise.catalog.events.ProductEventRelay
import com.shelfwise.catalog.product.ProductService
import com.shelfwise.catalog.quotes.BestQuote
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.annotation.Import
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.codec.ServerSentEvent
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.servlet.client.RestTestClient
import org.springframework.test.web.servlet.client.expectBody
import org.springframework.web.reactive.function.client.WebClient
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureRestTestClient
@Import(TestcontainersConfiguration::class)
class StreamingAndQuotesTest(
    @Autowired private val client: RestTestClient,
    @Autowired private val catalog: ProductService,
    @Autowired private val relay: ProductEventRelay,
    @LocalServerPort private val port: Int,
) {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun suppliers(registry: DynamicPropertyRegistry) {
            registry.add("spring.http.serviceclient.suppliers.base-url") { SupplierStub.baseUrl }
        }
    }

    @Test
    fun `the best quote comes from the suppliers that answered in time`() {
        val quote = client.get().uri("/api/products/SHW-1001/best-quote").exchange()
            .expectStatus().isOk
            .expectBody<BestQuote>().returnResult().responseBody!!

        assertEquals("SUP-001", quote.best?.supplier) // 16,100 paise, the lowest of the answers
        assertEquals(5, quote.asked)
        assertEquals(3, quote.answered) // SUP-003 timed out, SUP-005 returned 503
    }

    @Test
    fun `committed changes stream as server-sent events`(): Unit = runBlocking { // ': Unit': JUnit won't run a test that returns a value
        val events = WebClient.create("http://localhost:$port").get().uri("/api/products/changes")
            .retrieve()
            .bodyToFlux(object : ParameterizedTypeReference<ServerSentEvent<String>>() {})
            .asFlow()

        val first = async { withTimeout(10_000) { events.first() } }
        withTimeout(10_000) { relay.subscribers.first { it > 0 } } // the SSE request is now collecting
        catalog.reprice("SHW-1008", 13_000)

        val event = first.await()
        assertEquals("product-updated", event.event())
        assertTrue(""""pricePaise":13000""" in event.data()!!)
        catalog.reprice("SHW-1008", 12_500) // restore the seed price
    }
}
