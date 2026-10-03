package com.shelfwise.part05.functiontypes

// A function type is a real type: (Long) -> Long, not an interface you had to pick from java.util.function.
val festive: (Long) -> Long = { pricePaise -> pricePaise * 90 / 100 }

// A higher-order function takes (or returns) a function.
fun applyRule(pricePaise: Long, rule: (Long) -> Long): Long = rule(pricePaise)

// Returning a function: each call builds a new rule that remembers its 'percent'.
fun discountOf(percent: Int): (Long) -> Long = { it * (100 - percent) / 100 }

// Parameter names in a function type are documentation only.
fun describe(pricePaise: Long, format: (paise: Long, currency: String) -> String): String =
    format(pricePaise, "INR")

fun main() {
    println(festive(16_500)) // -> 14850
    println(applyRule(16_500, festive)) // -> 14850

    // Trailing lambda: the last function argument moves outside the parentheses.
    println(applyRule(16_500) { it - 1_000 }) // -> 15500

    // 'it' is the implicit name of a single parameter; name it when the lambda isn't tiny.
    println(applyRule(16_500) { price -> if (price > 10_000) price - 500 else price }) // -> 16000

    val clearance = discountOf(40)
    println(clearance(16_500)) // -> 9900

    // The last expression is the lambda's value. No 'return'.
    println(describe(16_550) { paise, currency -> "$currency ${paise / 100}.${paise % 100}" }) // -> INR 165.50

    // A nullable function type is called with ?.invoke()
    val optionalRule: ((Long) -> Long)? = null
    println(optionalRule?.invoke(16_500) ?: 16_500) // -> 16500
}
