package com.shelfwise.catalog

import org.junit.jupiter.api.Test
import tools.jackson.databind.DatabindException
import tools.jackson.module.kotlin.jacksonObjectMapper
import tools.jackson.module.kotlin.readValue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// Part 4's promise: what happens to a validating value class when a framework builds it.
@JvmInline
value class Sku(val value: String) {
    init {
        require(value.matches(Regex("SHW-\\d{4}"))) { "bad SKU: $value" }
    }
}

data class SkuRequest(val sku: Sku)

class ValueClassBoundaryTest {
    @Test
    fun `jackson-module-kotlin runs init`() {
        val mapper = jacksonObjectMapper()

        assertEquals(Sku("SHW-1001"), mapper.readValue<SkuRequest>("""{"sku":"SHW-1001"}""").sku)
        val failure = assertFailsWith<DatabindException> { mapper.readValue<SkuRequest>("""{"sku":"not a sku"}""") }
        assertTrue(failure.message!!.startsWith("bad SKU: not a sku"))
    }

    @Test
    fun `plain reflection through box-impl does not`() {
        // box-impl wraps an already-validated value; frameworks that find it by reflection skip init.
        val boxImpl = Sku::class.java.getDeclaredMethod("box-impl", String::class.java)

        val sku = boxImpl.invoke(null, "not a sku") as Sku

        assertEquals("not a sku", sku.value)
    }

    @Test
    fun `neither does the private JVM constructor`() {
        // init lives only in the static constructor-impl; the JVM constructor just stores the value.
        val constructor = Sku::class.java.getDeclaredConstructor(String::class.java).apply { isAccessible = true }

        assertEquals("nope", constructor.newInstance("nope").value)
    }
}
