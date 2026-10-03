package com.shelfwise.story

import jakarta.persistence.Entity
import jakarta.persistence.FetchType
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import jakarta.persistence.ManyToOne
import jakarta.persistence.OneToMany
import jakarta.persistence.Table
import jakarta.persistence.Transient

// Kabir's first attempt: entities as data classes, the way he'd model a DTO.
@Entity
@Table(name = "naive_suppliers")
data class NaiveSupplier(
    @Id @GeneratedValue var id: Long? = null,
    val code: String,
    @OneToMany(mappedBy = "supplier") val products: MutableList<NaiveProduct> = mutableListOf(),
)

@Entity
@Table(name = "naive_products")
data class NaiveProduct(
    @Id @GeneratedValue var id: Long? = null,
    val sku: String,
    var pricePaise: Long,
    @ManyToOne(fetch = FetchType.LAZY) val supplier: NaiveSupplier,
)

// Not a data class, and still a trap: Hibernate creates entities through the no-arg constructor,
// which doesn't run property initialisers unless noArg { invokeInitializers = true }.
@Entity
@Table(name = "naive_labels")
class ShelfLabel(
    @Id val sku: String,
    var text: String,
) {
    @Transient
    val printedBy: String = "label-service" // not a column, so nothing fills it in after loading
}
