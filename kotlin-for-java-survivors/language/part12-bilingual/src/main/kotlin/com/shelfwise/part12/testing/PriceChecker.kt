package com.shelfwise.part12.testing

// The code under test in this part's test suite.
interface SupplierClient {
    suspend fun quote(sku: String): Long
}

class PriceChecker(private val supplier: SupplierClient, private val tolerancePercent: Int = 5) {
    suspend fun isCompetitive(sku: String, ourPaise: Long): Boolean {
        val theirs = supplier.quote(sku)
        require(ourPaise > 0 && theirs > 0) { "prices must be positive" }
        return ourPaise <= theirs * (100 + tolerancePercent) / 100
    }
}
