package com.shelfwise.part01.functions

// One function with defaults replaces a telescope of Java overloads.
fun discounted(pricePaise: Long, percent: Int = 10, roundToRupee: Boolean = false): Long {
    val raw = pricePaise * (100 - percent) / 100
    return if (roundToRupee) raw / 100 * 100 else raw
}

// Expression body: '=' instead of braces and return. The return type (Unit) is inferred.
fun log(message: String) = println("[labels] $message")

fun main() {
    println(discounted(19_900)) // -> 17910
    println(discounted(19_900, percent = 25)) // -> 14925
    println(discounted(19_900, roundToRupee = true)) // -> 17900
    println(discounted(roundToRupee = true, percent = 5, pricePaise = 19_900)) // -> 18900
    // discounted(percent = 5, 19_900)  // error: mixing named and positional arguments is not allowed unless the order of the arguments matches the order of the parameters.

    // Unit is a real object, not a keyword like void.
    val result: Unit = log("printed") // -> [labels] printed
    println(result) // -> kotlin.Unit
}
