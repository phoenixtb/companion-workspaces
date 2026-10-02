package com.shelfwise.part01.strings

fun main() {
    val scanned = StringBuilder("SKU-").append(42).toString() // built at runtime: a new String object
    val expected = "SKU-42"

    println(scanned == expected) // -> true
    println(scanned === expected) // -> false
}
