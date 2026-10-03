package com.shelfwise.catalog.product

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.UuidGenerator
import java.util.UUID

@Entity
@Table(name = "suppliers")
class Supplier(
    @Column(nullable = false, unique = true, updatable = false)
    val code: String,
    var name: String,
) {
    @Id
    @GeneratedValue
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    var id: UUID? = null
        protected set

    // Identity is the business key: stable before and after the entity is saved.
    override fun equals(other: Any?) = this === other || (other is Supplier && code == other.code)

    override fun hashCode() = code.hashCode()

    override fun toString() = "Supplier(code=$code)"
}
