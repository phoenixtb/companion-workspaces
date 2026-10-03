package com.shelfwise.part04.aliases

// A type alias is a second name for an existing type. It adds readability, not safety.
typealias SkuCode = String
typealias PriceRule = (Long) -> Long

class Catalog(private val entries: Map<SkuCode, Long>) {
    // Nested type alias (Stable since 2.3.0): scoped to the class that gives it meaning.
    typealias Index = Map<SkuCode, Long>

    fun index(): Index = entries
}

fun applyRules(pricePaise: Long, rules: List<PriceRule>): Long {
    var result = pricePaise
    for (rule in rules) result = rule(result)
    return result
}

fun main() {
    val code: SkuCode = "this is not a SKU at all"
    val plain: String = code // interchangeable with String, in both directions
    println(plain.length) // -> 24

    val index: Catalog.Index = Catalog(mapOf("SHW-1001" to 16_500L)).index()
    println(index) // -> {SHW-1001=16500}

    val festive: PriceRule = { it * 90 / 100 }
    val roundDown: PriceRule = { it / 100 * 100 }
    println(applyRules(16_550, listOf(festive, roundDown))) // -> 14800
}
