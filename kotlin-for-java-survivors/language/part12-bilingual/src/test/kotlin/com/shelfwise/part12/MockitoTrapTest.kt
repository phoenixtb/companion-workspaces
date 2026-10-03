package com.shelfwise.part12

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

interface PriceLookup {
    fun price(sku: String): Long
}

class MockitoTrapTest {
    @AfterEach
    fun clearMockitoState() {
        runCatching { Mockito.validateMockitoUsage() } // the failed stubbing leaves a matcher behind
    }

    @Test
    fun `Mockito's any() returns null, and Kotlin checks it before the call`() {
        val lookup = Mockito.mock(PriceLookup::class.java)

        val failure = assertFailsWith<NullPointerException> {
            Mockito.`when`(lookup.price(ArgumentMatchers.any())).thenReturn(16_500) // `when` is a Kotlin keyword
        }

        assertEquals("any(...) must not be null", failure.message)
    }
}
