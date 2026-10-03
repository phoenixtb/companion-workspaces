package com.shelfwise.part12

import com.shelfwise.part12.testing.PriceChecker
import com.shelfwise.part12.testing.SupplierClient
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class PriceCheckerTest {
    // MockK: mocks final Kotlin classes and suspend functions; no open-for-testing required.
    private val supplier = mockk<SupplierClient>()
    private val checker = PriceChecker(supplier)

    @Test
    fun `a price within 5 percent of the supplier's is competitive`() = runTest {
        coEvery { supplier.quote("SHW-1001") } returns 16_000 // coEvery: stub a suspend function

        checker.isCompetitive("SHW-1001", ourPaise = 16_800) shouldBe true // Kotest assertion
        checker.isCompetitive("SHW-1001", ourPaise = 16_801) shouldBe false

        coVerify(exactly = 2) { supplier.quote("SHW-1001") }
    }

    @Test
    fun `non-positive prices are rejected`() = runTest {
        coEvery { supplier.quote(any()) } returns 0

        val error = shouldThrow<IllegalArgumentException> { checker.isCompetitive("SHW-1001", 100) }
        error.message shouldContain "prices must be positive"
    }
}
