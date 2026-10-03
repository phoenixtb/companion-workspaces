package com.shelfwise.part12.api

import java.util.Collections

// The same class, annotated for Java callers.
class PriceList
@JvmOverloads // generates PriceList(), PriceList(String) and PriceList(String, long) for Java
constructor(
    val currency: String = DEFAULT_CURRENCY,
    val roundingPaise: Long = 100,
) {
    private val prices = mutableMapOf<String, Long>()

    operator fun set(sku: String, pricePaise: Long) {
        prices[sku] = pricePaise
    }

    @JvmOverloads // generates priceOf(String) and priceOf(String, int) for Java
    fun priceOf(sku: String, discountPercent: Int = 0): Long {
        val base = prices.getValue(sku) * (100 - discountPercent) / 100
        return base / roundingPaise * roundingPaise
    }

    companion object {
        const val DEFAULT_CURRENCY = "INR" // already a static final field

        @JvmField // a static field instead of PriceList.Companion.getSUPPORTED_CURRENCIES()
        val SUPPORTED_CURRENCIES: List<String> = Collections.unmodifiableList(listOf("INR", "NPR"))

        @JvmStatic // a static method on PriceList, not only on PriceList.Companion
        fun forCurrency(currency: String) = PriceList(currency)
    }
}

// A real java.lang.Record, so Java can use record patterns on it.
@JvmRecord
data class PriceTag(val sku: String, val pricePaise: Long)

object Rounding {
    @JvmStatic // without it, Java writes Rounding.INSTANCE.toRupee(...)
    fun toRupee(paise: Long): Long = paise / 100 * 100
}
