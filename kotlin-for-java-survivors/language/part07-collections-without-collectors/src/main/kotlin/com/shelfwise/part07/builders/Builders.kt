package com.shelfwise.part07.builders

fun main() {
    val skus = listOf("SHW-1001", "SHW-2040")
    val prices = mapOf("SHW-1001" to 16_500L, "SHW-2040" to 72_000L) // 'to' builds a Pair

    // Builders: mutate inside, read-only outside. No temporary mutable variable leaks out.
    val shelfLabels = buildList {
        add("AISLE 7")
        for (sku in skus) add("$sku ₹${prices.getValue(sku) / 100}")
        if (size > 2) add("END")
    }
    println(shelfLabels) // -> [AISLE 7, SHW-1001 ₹165, SHW-2040 ₹720, END]

    val bySku = buildMap {
        put("SHW-1001", "Toor Dal 1kg")
        putAll(mapOf("SHW-2040" to "Basmati 5kg"))
    }
    println(bySku) // -> {SHW-1001=Toor Dal 1kg, SHW-2040=Basmati 5kg}

    // Size-and-initializer constructors, like IntStream.range(...).mapToObj(...).toList().
    println(List(3) { "slot-$it" }) // -> [slot-0, slot-1, slot-2]

    // [] returns null for a missing key; getValue (used above) throws instead.
    println(prices["SHW-9999"]) // -> null
}
