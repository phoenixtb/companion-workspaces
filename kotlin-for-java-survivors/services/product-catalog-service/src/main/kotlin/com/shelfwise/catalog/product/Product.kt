package com.shelfwise.catalog.product

import jakarta.persistence.CollectionTable
import jakarta.persistence.Column
import jakarta.persistence.ElementCollection
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.FetchType
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.ManyToOne
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.hibernate.annotations.UuidGenerator
import java.time.Instant
import java.util.UUID

enum class Category { PASTA_AND_GRAINS, DAIRY, BEVERAGES, SNACKS, BREAKFAST, PERSONAL_CARE, SMALL_APPLIANCES }

// A plain class, not a data class: an entity has an identity, a lifecycle and lazy associations.
// plugin.jpa makes it open (for Hibernate's proxies) and gives it a no-arg constructor.
@Entity
@Table(name = "products")
class Product(
    @Column(nullable = false, unique = true, updatable = false)
    val sku: String,
    var name: String,
    var description: String,
    @Enumerated(EnumType.STRING)
    var category: Category,
    var pricePaise: Long,
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "supplier_id")
    var supplier: Supplier,
    @ElementCollection
    @CollectionTable(name = "product_dietary_tags")
    @Column(name = "tag")
    var dietaryTags: MutableSet<String> = mutableSetOf(),
    val createdAt: Instant,
) {
    @Id
    @GeneratedValue
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    var id: UUID? = null
        protected set // assigned by Hibernate on persist; nobody else should write it

    @Version
    var version: Long = 0
        protected set

    override fun equals(other: Any?) = this === other || (other is Product && sku == other.sku)

    override fun hashCode() = sku.hashCode()

    // Never touch lazy associations here: toString runs in log lines, debuggers and test failures.
    override fun toString() = "Product(sku=$sku, name=$name)"
}
