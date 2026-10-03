package com.shelfwise.catalog.product

import com.shelfwise.events.ProductCreated
import com.shelfwise.events.ProductDeleted
import com.shelfwise.events.ProductSnapshot
import com.shelfwise.events.ProductUpdated
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.Reader
import java.time.Clock
import kotlin.time.toKotlinInstant
import kotlin.uuid.Uuid

data class NewProduct(
    val sku: String,
    val name: String,
    val description: String,
    val category: Category,
    val pricePaise: Long,
    val supplierCode: String,
    val dietaryTags: Set<String>,
)

@Service
@Transactional(readOnly = true)
class ProductService(
    private val products: ProductRepository,
    private val suppliers: SupplierRepository,
    private val clock: Clock,
    private val events: ApplicationEventPublisher,
) {
    fun bySku(sku: String): Product = products.findBySku(sku) ?: throw ProductNotFound(sku)

    fun search(category: Category, maxPricePaise: Long): List<Product> =
        products.findByCategoryAndPricePaiseLessThanEqualOrderByPricePaise(category, maxPricePaise)

    @Transactional
    fun create(new: NewProduct): Product {
        if (products.existsBySku(new.sku)) throw DuplicateSku(new.sku)
        val supplier = suppliers.findByCode(new.supplierCode) ?: throw UnknownSupplier(new.supplierCode)
        val saved = products.save(
            Product(
                sku = new.sku,
                name = new.name,
                description = new.description,
                category = new.category,
                pricePaise = new.pricePaise,
                supplier = supplier,
                dietaryTags = new.dietaryTags.toMutableSet(),
                createdAt = clock.instant(),
            ),
        )
        events.publishEvent(ProductCreated(Uuid.random(), saved.sku, now(), saved.snapshot()))
        return saved
    }

    // Each write publishes an event inside its transaction; ProductEventRelay delivers it after commit.
    @Transactional
    fun reprice(sku: String, pricePaise: Long): Product = bySku(sku).apply {
        this.pricePaise = pricePaise
        events.publishEvent(ProductUpdated(Uuid.random(), sku, now(), snapshot()))
    }

    @Transactional
    fun delete(sku: String) {
        products.delete(bySku(sku))
        events.publishEvent(ProductDeleted(Uuid.random(), sku, now()))
    }

    // Rows that never went through create(), such as Flyway's seed data, have no events. A republish
    // emits every product's current state, which idempotent consumers absorb safely.
    @Transactional
    fun republishAll(): Int = products.findAll().onEach { product ->
        events.publishEvent(ProductUpdated(Uuid.random(), product.sku, now(), product.snapshot()))
    }.size

    private fun now() = clock.instant().toKotlinInstant()

    private fun Product.snapshot() =
        ProductSnapshot(sku, name, description, category.name, pricePaise, supplier.name, dietaryTags.sorted())

    // Applies a supplier's "SKU,pricePaise" price list. If reading fails halfway with an IOException, a
    // checked exception, the lines already applied must be undone: RollbackOn.ALL_EXCEPTIONS makes it so.
    @Transactional
    fun importPriceList(priceList: Reader): Int = priceList.buffered().useLines { lines ->
        lines.map { it.split(',') }.onEach { (sku, paise) -> reprice(sku.trim(), paise.trim().toLong()) }.count()
    }
}
