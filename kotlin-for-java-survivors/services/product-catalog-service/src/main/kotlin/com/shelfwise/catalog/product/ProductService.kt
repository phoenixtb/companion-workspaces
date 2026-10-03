package com.shelfwise.catalog.product

import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.Reader
import java.time.Clock

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
) {
    fun bySku(sku: String): Product = products.findBySku(sku) ?: throw ProductNotFound(sku)

    fun search(category: Category, maxPricePaise: Long): List<Product> =
        products.findByCategoryAndPricePaiseLessThanEqualOrderByPricePaise(category, maxPricePaise)

    @Transactional
    fun create(new: NewProduct): Product {
        if (products.existsBySku(new.sku)) throw DuplicateSku(new.sku)
        val supplier = suppliers.findByCode(new.supplierCode) ?: throw UnknownSupplier(new.supplierCode)
        return products.save(
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
    }

    @Transactional
    fun reprice(sku: String, pricePaise: Long): Product = bySku(sku).apply { this.pricePaise = pricePaise }

    // Applies a supplier's "SKU,pricePaise" price list. If reading fails halfway with an IOException, a
    // checked exception, the lines already applied must be undone: RollbackOn.ALL_EXCEPTIONS makes it so.
    @Transactional
    fun importPriceList(priceList: Reader): Int = priceList.buffered().useLines { lines ->
        lines.map { it.split(',') }.onEach { (sku, paise) -> reprice(sku.trim(), paise.trim().toLong()) }.count()
    }
}
