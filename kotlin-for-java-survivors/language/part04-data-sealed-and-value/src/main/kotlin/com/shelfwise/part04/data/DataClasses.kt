package com.shelfwise.part04.data

data class Price(val paise: Long, val currency: String = "INR")

data class ProductLine(val sku: String, val name: String, val price: Price) {
    // Declared in the body, so not part of equals, hashCode, toString or copy.
    var lastAudit: String = "never"
}

fun main() {
    val a = ProductLine("SHW-1001", "Toor Dal 1kg", Price(16_500))
    val b = ProductLine("SHW-1001", "Toor Dal 1kg", Price(16_500))

    println(a) // -> ProductLine(sku=SHW-1001, name=Toor Dal 1kg, price=Price(paise=16500, currency=INR))
    println(a == b) // -> true
    println(setOf(a, b).size) // -> 1

    // copy(): a new instance with some properties changed, by name. The original is untouched.
    val discounted = a.copy(price = a.price.copy(paise = 14_900))
    println(discounted.price) // -> Price(paise=14900, currency=INR)
    println(a.price.paise) // -> 16500

    b.lastAudit = "2026-10-01"
    println(a == b) // -> true

    // Positional destructuring: component1(), component2(), in constructor order.
    val (sku, name) = a
    println("$sku / $name") // -> SHW-1001 / Toor Dal 1kg
}
