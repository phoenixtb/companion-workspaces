package com.shelfwise.part12.naive

// The first version of the library: idiomatic Kotlin, written without Java callers in mind.
class PriceList(val currency: String = DEFAULT_CURRENCY, val roundingPaise: Long = 100) {
    private val prices = mutableMapOf<String, Long>()

    operator fun set(sku: String, pricePaise: Long) {
        prices[sku] = pricePaise
    }

    fun priceOf(sku: String, discountPercent: Int = 0): Long {
        val base = prices.getValue(sku) * (100 - discountPercent) / 100
        return base / roundingPaise * roundingPaise
    }

    companion object {
        const val DEFAULT_CURRENCY = "INR"
        val SUPPORTED_CURRENCIES = listOf("INR", "NPR")

        fun forCurrency(currency: String) = PriceList(currency)
    }
}

object Rounding {
    fun toRupee(paise: Long): Long = paise / 100 * 100
}

@JvmInline
value class Sku(val value: String)

fun shelfLabel(sku: Sku, pricePaise: Long): String = "${sku.value} ₹${pricePaise / 100}"

fun Long.formatPaise(): String = "₹${this / 100}.${(this % 100).toString().padStart(2, '0')}"

fun main() {
    // From Kotlin, all of it reads well.
    val list = PriceList.forCurrency("INR")
    list["SHW-1001"] = 16_550
    println(list.priceOf("SHW-1001", discountPercent = 10)) // -> 14800
    println(shelfLabel(Sku("SHW-1001"), 14_800)) // -> SHW-1001 ₹148
    println(14_850L.formatPaise()) // -> ₹148.50
}
