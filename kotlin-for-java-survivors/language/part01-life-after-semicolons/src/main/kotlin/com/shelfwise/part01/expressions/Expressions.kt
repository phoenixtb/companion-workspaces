package com.shelfwise.part01.expressions

// when without a subject: an if/else-if chain that produces a value.
fun stockBand(units: Int): String = when {
    units == 0 -> "OUT"
    units < 10 -> "LOW"
    else -> "OK"
}

// when with a subject: constants, several values per branch, ranges and collections.
fun aisleFor(category: String): Int = when (category) {
    "Dairy", "Eggs" -> 1
    "Bakery" -> 2
    in setOf("Frozen", "Ice Cream") -> 7
    else -> 99
}

// Type checks smart-cast the subject inside the branch: no explicit cast needed.
fun describe(value: Any): String = when (value) {
    is String -> "text of length ${value.length}"
    is Int -> "number ${value + 1}"
    else -> "something else"
}

// Guard conditions (Stable since Kotlin 2.2): Java 21's 'case Integer i when i < 0'.
fun quantityLabel(value: Any): String = when (value) {
    is Int if value < 0 -> "invalid quantity"
    is Int -> "$value units"
    else -> "unknown"
}

// try is an expression too: its value is the last expression of try or of the catch that ran.
fun parseQuantity(raw: String): Int = try {
    raw.trim().toInt()
} catch (e: NumberFormatException) {
    0
}

fun main() {
    val units = 4
    val shelfMessage = if (units > 0) "In stock" else "Sold out" // no ?: ternary needed
    println(shelfMessage) // -> In stock

    println(stockBand(0)) // -> OUT
    println(stockBand(units)) // -> LOW
    println(aisleFor("Eggs")) // -> 1
    println(aisleFor("Ice Cream")) // -> 7
    println(describe("Atta")) // -> text of length 4
    println(describe(41)) // -> number 42
    println(quantityLabel(-3)) // -> invalid quantity
    println(quantityLabel(12)) // -> 12 units
    println(parseQuantity(" 12 ")) // -> 12
    println(parseQuantity("twelve")) // -> 0

    // val label = if (units > 0) "on"  // error: 'if' must have both main and 'else' branches when used as an expression.
}
