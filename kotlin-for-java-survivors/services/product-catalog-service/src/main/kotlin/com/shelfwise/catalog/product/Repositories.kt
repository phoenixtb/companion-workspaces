package com.shelfwise.catalog.product

import org.springframework.data.jpa.repository.EntityGraph
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface ProductRepository : JpaRepository<Product, UUID> {
    // A nullable return type is enough: Spring Data returns null instead of throwing or wrapping in Optional.
    @EntityGraph(attributePaths = ["supplier", "dietaryTags"])
    fun findBySku(sku: String): Product?

    @EntityGraph(attributePaths = ["supplier", "dietaryTags"])
    fun findByCategoryAndPricePaiseLessThanEqualOrderByPricePaise(category: Category, maxPricePaise: Long): List<Product>

    fun existsBySku(sku: String): Boolean
}

interface SupplierRepository : JpaRepository<Supplier, UUID> {
    fun findByCode(code: String): Supplier?
}
