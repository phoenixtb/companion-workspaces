package com.shelfwise.part02.equality

fun main() {
    // Nullable Ints are java.lang.Integer objects, with Java's -128..127 cache.
    val smallA: Int? = 100
    val smallB: Int? = 100
    val bigA: Int? = 1000
    val bigB: Int? = 1000
    // The compiler warns on these: identity equality for arguments of types 'Int?' and 'Int?' is prohibited.
    println(smallA === smallB) // -> true
    println(bigA === bigB) // -> false
    println(bigA == bigB) // -> true

    // Arrays are JVM arrays: == compares references (with a compiler warning). Use contentEquals.
    val todayCounts = intArrayOf(12, 0, 7)
    val expected = intArrayOf(12, 0, 7)
    println(todayCounts.contentEquals(expected)) // -> true
    println(todayCounts.contentToString()) // -> [12, 0, 7]

    // is / !is: instanceof that also smart-casts.
    val payload: Any = listOf("gluten", "nuts")
    if (payload is List<*> && payload.isNotEmpty()) println("${payload.size} allergens") // -> 2 allergens
    println(payload !is String) // -> true
}
