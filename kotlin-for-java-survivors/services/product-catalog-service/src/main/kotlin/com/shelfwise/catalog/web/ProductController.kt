package com.shelfwise.catalog.web

import com.shelfwise.catalog.product.Category
import com.shelfwise.catalog.product.ProductService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import java.net.URI

@RestController
@RequestMapping("/api/products")
class ProductController(private val catalog: ProductService) {
    @GetMapping("/{sku}")
    fun get(@PathVariable sku: String): ProductResponse = catalog.bySku(sku).toResponse()

    // A nullable parameter is an optional request parameter: Spring reads Kotlin's nullability.
    @GetMapping
    fun search(@RequestParam category: Category, @RequestParam maxPricePaise: Long?): List<ProductResponse> =
        catalog.search(category, maxPricePaise ?: Long.MAX_VALUE).map { it.toResponse() }

    @PostMapping
    fun create(@Valid @RequestBody request: CreateProductRequest): ResponseEntity<ProductResponse> {
        val product = catalog.create(request.toNewProduct())
        return ResponseEntity.created(URI.create("/api/products/${product.sku}")).body(product.toResponse())
    }

    // Backfill for consumers (Part 15): one product-updated event per product. An admin endpoint.
    @PostMapping("/republish")
    fun republish() = mapOf("republished" to catalog.republishAll())

    @DeleteMapping("/{sku}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun delete(@PathVariable sku: String) = catalog.delete(sku)

    @PutMapping("/{sku}/price")
    fun reprice(@PathVariable sku: String, @Valid @RequestBody request: RepriceRequest): ProductResponse =
        catalog.reprice(sku, request.pricePaise).toResponse()
}
