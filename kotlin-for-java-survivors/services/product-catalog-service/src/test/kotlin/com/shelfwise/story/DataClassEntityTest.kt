package com.shelfwise.story

import com.shelfwise.catalog.TestcontainersConfiguration
import jakarta.persistence.EntityManager
import org.hibernate.LazyInitializationException
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.context.annotation.Import
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionTemplate
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@DataJpaTest(properties = ["spring.jpa.hibernate.ddl-auto=create-drop", "spring.flyway.enabled=false"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration::class)
class DataClassEntityTest(
    @Autowired private val em: EntityManager,
    @Autowired private val transactions: PlatformTransactionManager,
) {
    private fun saveOne(): Long {
        val supplier = NaiveSupplier(code = "SUP-001")
        val product = NaiveProduct(sku = "SHW-1001", pricePaise = 16_500, supplier = supplier)
        supplier.products += product
        em.persist(supplier)
        em.persist(product)
        em.flush()
        em.clear()
        return product.id!!
    }

    @Test
    fun `toString walks the bidirectional association until the stack runs out`() {
        val product = em.find(NaiveProduct::class.java, saveOne())

        // product -> supplier (initialised by the proxy) -> products -> product -> ...
        val failure = assertFailsWith<StackOverflowError> { product.toString() }
        assertTrue(failure.stackTrace.size > 1_000) // thousands of toString frames
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun `outside the transaction, toString touches a proxy with no session`() {
        val tx = TransactionTemplate(transactions)
        val id = tx.execute { saveOne() } // Spring 7 (JSpecify): a non-null T for a non-null lambda, no !!
        val product = tx.execute { em.find(NaiveProduct::class.java, id) }!! // Jakarta API: a platform type, so !! here

        val failure = assertFailsWith<LazyInitializationException> { product.toString() }
        assertTrue(failure.message!!.startsWith("Could not initialize proxy [com.shelfwise.story.NaiveSupplier#"))
        assertTrue(failure.message!!.endsWith("] - no session"))
    }

    @Test
    fun `hashCode changes when the id is assigned`() {
        val supplier = NaiveSupplier(code = "SUP-002")
        em.persist(supplier)
        val product = NaiveProduct(sku = "SHW-2002", pricePaise = 9_000, supplier = supplier)
        val inCart = hashSetOf(product)

        em.persist(product) // assigns id, which is part of equals and hashCode

        assertFalse(product in inCart)
    }

    @Test
    fun `initialisers don't run when Hibernate creates the entity`() {
        em.persist(ShelfLabel(sku = "SHW-1001", text = "Penne ₹165"))
        em.flush()
        em.clear()

        val loaded = em.find(ShelfLabel::class.java, "SHW-1001")
        val printedBy: String? = loaded.printedBy // declared non-null, and null all the same

        assertNull(printedBy)
    }
}
