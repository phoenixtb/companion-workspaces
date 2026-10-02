package com.shelfwise.part02.types

// Nothing: the return type of a function that never returns normally.
fun fail(reason: String): Nothing = throw IllegalArgumentException(reason)

fun parseSku(raw: String?): String {
    // Because fail() returns Nothing, the Elvis expression has type String, not String?.
    val sku = raw?.trim()?.takeIf { it.isNotEmpty() } ?: fail("blank SKU")
    return sku.uppercase()
}

fun parseQuantity(raw: String): Int {
    // error() is the standard library's fail(): it throws IllegalStateException and returns Nothing.
    val quantity = raw.toIntOrNull() ?: error("not a number: '$raw'")
    // 'if' with a throwing branch still has type Int: throw is an expression of type Nothing.
    return if (quantity >= 0) quantity else throw IllegalArgumentException("negative: $quantity")
}

fun main() {
    println(parseSku("  sup-7781 ")) // -> SUP-7781
    try {
        parseSku("   ")
    } catch (e: IllegalArgumentException) {
        println(e.message) // -> blank SKU
    }
    try {
        parseQuantity("twelve")
    } catch (e: IllegalStateException) {
        println(e.message) // -> not a number: 'twelve'
    }

    // Any is the root of non-null types; Any? is the root of everything.
    val things: List<Any?> = listOf("rice", 5, null)
    println(things.map { it?.javaClass?.simpleName ?: "null" }) // -> [String, Integer, null]
    // things.first()!!.getClass()  // error: unresolved reference 'getClass' on receiver of type 'Any'.
}
