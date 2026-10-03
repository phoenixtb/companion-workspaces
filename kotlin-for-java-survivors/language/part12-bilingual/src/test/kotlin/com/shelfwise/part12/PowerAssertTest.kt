package com.shelfwise.part12

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class PowerAssertTest {
    @Test
    fun `power-assert shows every value in a failed assertion`() {
        val discount = 95
        val maxDiscount = 90

        val failure = assertFailsWith<AssertionError> {
            assertTrue(discount in 0..maxDiscount) // a plain boolean assertion, no message written
        }

        assertEquals(
            """
            |assertTrue(discount in 0..maxDiscount)
            |           |        |   | |
            |           95       |   | 90
            |                    |   0..90
            |                    false
            """.trimMargin(),
            failure.message?.trim(),
        )
    }
}
