package com.shelfwise.catalog

import com.shelfwise.catalog.web.ProductResponse
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.test.web.servlet.client.RestTestClient
import org.springframework.test.web.servlet.client.expectBody
import kotlin.test.assertEquals

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureRestTestClient
@Import(TestcontainersConfiguration::class, ProductApiTest.ThreadProbe::class)
class ProductApiTest(@Autowired private val client: RestTestClient) {
    @Test
    fun `a seeded product comes back with its supplier`() {
        val product = client.get().uri("/api/products/SHW-1001").exchange()
            .expectStatus().isOk
            .expectBody<ProductResponse>().returnResult().responseBody!!

        assertEquals("Durum Wheat Penne 500 g", product.name)
        assertEquals("₹165.00", product.price)
        assertEquals("Banyan Agro Supplies", product.supplier)
    }

    @Test
    fun `an unknown SKU is a 404 problem detail`() {
        client.get().uri("/api/products/SHW-9999").exchange()
            .expectStatus().isNotFound
            .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
            .expectBody()
            .jsonPath("$.title").isEqualTo("Product not found")
            .jsonPath("$.sku").isEqualTo("SHW-9999")
    }

    @Test
    fun `a valid product is created`() {
        client.post().uri("/api/products").contentType(MediaType.APPLICATION_JSON)
            .body("""{"sku":"SHW-1101","name":"Cold Brew Coffee 250 ml","category":"BEVERAGES","pricePaise":12000,"supplierCode":"SUP-004"}""")
            .exchange()
            .expectStatus().isCreated
            .expectHeader().location("/api/products/SHW-1101")
            .expectBody()
            .jsonPath("$.description").isEqualTo("") // a Kotlin default value, applied by jackson-module-kotlin
            .jsonPath("$.supplier").isEqualTo("Shri Krishna Consumer Goods")
    }

    @Test
    fun `constraints without use-site targets are validated`() {
        client.post().uri("/api/products").contentType(MediaType.APPLICATION_JSON)
            .body("""{"sku":"bad","name":" ","category":"SNACKS","pricePaise":-5,"supplierCode":"SUP-001"}""")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody()
            .jsonPath("$.errors.sku").isEqualTo("must look like SHW-1234")
            .jsonPath("$.errors.name").isEqualTo("must not be blank")
            .jsonPath("$.errors.pricePaise").isEqualTo("must be greater than 0")
    }

    @Test
    fun `a missing non-null property is a 400, not a null in the object`() {
        client.post().uri("/api/products").contentType(MediaType.APPLICATION_JSON)
            .body("""{"sku":"SHW-1102","name":"Masala Oats","pricePaise":9000,"supplierCode":"SUP-002"}""")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody()
            .jsonPath("$.detail").isEqualTo("Failed to read request")
    }

    @Test
    fun `a duplicate SKU is a conflict`() {
        client.post().uri("/api/products").contentType(MediaType.APPLICATION_JSON)
            .body("""{"sku":"SHW-1001","name":"Another Penne","category":"PASTA_AND_GRAINS","pricePaise":100,"supplierCode":"SUP-001"}""")
            .exchange()
            .expectStatus().isEqualTo(409)
            .expectBody()
            .jsonPath("$.detail").isEqualTo("A product with SKU SHW-1001 already exists")
    }

    @Test
    fun `search by category and maximum price`() {
        client.get().uri("/api/products?category=PASTA_AND_GRAINS&maxPricePaise=20000").exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$[*].sku").isEqualTo(listOf("SHW-1005", "SHW-1002", "SHW-1001", "SHW-1003"))
    }

    @Test
    fun `requests run on virtual threads`() {
        client.get().uri("/test/thread").exchange()
            .expectStatus().isOk
            .expectBody<String>().isEqualTo("virtual=true")
    }

    @RestController
    class ThreadProbe {
        @GetMapping("/test/thread")
        fun thread() = "virtual=${Thread.currentThread().isVirtual}"
    }
}
