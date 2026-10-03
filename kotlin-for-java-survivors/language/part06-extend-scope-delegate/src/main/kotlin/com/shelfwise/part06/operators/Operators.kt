package com.shelfwise.part06.operators

@JvmInline
value class Paise(val amount: Long) : Comparable<Paise> {
    operator fun plus(other: Paise) = Paise(amount + other.amount)

    operator fun times(quantity: Int) = Paise(amount * quantity)

    override fun compareTo(other: Paise) = amount.compareTo(other.amount)

    override fun toString() = "₹${amount / 100}.${(amount % 100).toString().padStart(2, '0')}"
}

// infix: a two-argument call written without dots and parentheses.
infix fun Paise.percentOff(percent: Int) = Paise(amount * (100 - percent) / 100)

class PriceBook {
    private val prices = mutableMapOf<String, Paise>()

    operator fun get(sku: String): Paise? = prices[sku]

    operator fun set(sku: String, price: Paise) {
        prices[sku] = price
    }

    operator fun contains(sku: String) = sku in prices
}

fun main() {
    val dal = Paise(16_500)
    val rice = Paise(72_000)

    println(dal + rice) // -> ₹885.00
    println(dal * 3) // -> ₹495.00
    println(dal < rice) // -> true
    println(dal percentOff 10) // -> ₹148.50

    val book = PriceBook()
    book["SHW-1001"] = dal // set(...)
    println(book["SHW-1001"]) // -> ₹165.00
    println("SHW-9999" in book) // -> false

    var basket = Paise(0)
    basket += dal // no plusAssign defined, so this is basket = basket + dal
    basket += rice
    println(basket) // -> ₹885.00
}
