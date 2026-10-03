package com.shelfwise.catalog

import com.shelfwise.catalog.product.ProductService
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@SpringBootTest
@Import(TestcontainersConfiguration::class)
class PriceImportTest(@Autowired private val catalog: ProductService) {
    @Test
    fun `a checked exception rolls the import back`() {
        val failure = assertFailsWith<IOException> { catalog.importPriceList(FailingAfter("SHW-1002,9000\n")) }

        assertEquals("connection reset by supplier", failure.message)
        assertEquals(8_500, catalog.bySku("SHW-1002").pricePaise) // the first line's change was rolled back
    }
}
