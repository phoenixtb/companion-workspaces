package com.shelfwise.catalog

import com.shelfwise.catalog.product.Category
import com.shelfwise.catalog.product.Product
import com.shelfwise.catalog.product.ProductRepository
import com.shelfwise.catalog.product.SupplierRepository
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.context.annotation.Import
import org.springframework.data.repository.findByIdOrNull
import org.springframework.orm.ObjectOptimisticLockingFailureException
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

// The same three checks against the real entity: a plain class keyed on its SKU.
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration::class)
class ProductEntityTest(
    @Autowired private val products: ProductRepository,
    @Autowired private val suppliers: SupplierRepository,
    @Autowired private val transactions: PlatformTransactionManager,
) {
    private fun newProduct(sku: String) = Product(
        sku = sku,
        name = "Masala Oats 500 g",
        description = "Rolled oats with a spice mix.",
        category = Category.BREAKFAST,
        pricePaise = 14_000,
        supplier = suppliers.findByCode("SUP-002")!!,
        createdAt = Instant.parse("2026-10-01T04:30:00Z"),
    )

    @Test
    fun `equality survives saving`() {
        val product = newProduct("SHW-1201")
        val inCart = hashSetOf(product)

        products.saveAndFlush(product)

        assertTrue(product in inCart)
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun `toString is safe outside the transaction`() {
        val tx = TransactionTemplate(transactions)
        val id = tx.execute { products.save(newProduct("SHW-1202")).id!! }
        val loaded = tx.execute { products.findByIdOrNull(id) }!! // supplier is still a lazy proxy here

        assertEquals("Product(sku=SHW-1202, name=Masala Oats 500 g)", loaded.toString())
        tx.execute { products.deleteById(id) }
    }

    @Test
    fun `findByIdOrNull returns null instead of an Optional`() {
        assertNull(products.findByIdOrNull(java.util.UUID.randomUUID()))
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun `a stale copy fails the optimistic lock`() {
        val tx = TransactionTemplate(transactions)
        val stale = tx.execute { products.findBySku("SHW-1011")!! } // version 0, now detached
        tx.execute { products.findBySku("SHW-1011")!!.pricePaise = 8_900 } // commits version 1

        stale.pricePaise = 9_900
        assertFailsWith<ObjectOptimisticLockingFailureException> { tx.execute { products.save(stale) } }
        tx.execute { products.findBySku("SHW-1011")!!.pricePaise = 8_500 } // restore the seed price
    }
}
