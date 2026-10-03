package com.shelfwise.part05.closures

class Promotion(val name: String, val minPaise: Long)

fun main() {
    val rules = listOf(Promotion("BULK", 50_000), Promotion("FESTIVE", 10_000), Promotion("STAFF", 0))

    // Java would reject this: 'applicable' is not effectively final. Kotlin captures the variable itself.
    var applicable = 0
    rules.forEach { if (16_500 >= it.minPaise) applicable++ }
    println(applicable) // -> 2

    // The lambda sees the variable, not a snapshot of its value.
    var percent = 10
    val discount = { pricePaise: Long -> pricePaise * (100 - percent) / 100 }
    percent = 50
    println(discount(16_500)) // -> 8250
}
