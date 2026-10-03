package com.shelfwise.part08.erasure

// Both erase to total(java.util.List): @JvmName gives them different JVM names.
@JvmName("totalPaise")
fun total(prices: List<Long>): Long = prices.sum()

@JvmName("totalUnits")
fun total(units: List<Int>): Long = units.sum().toLong()

fun main() {
    val prices: List<Long> = listOf(16_500, 3_050)
    val units: List<Int> = listOf(120, 35)
    println(total(prices)) // -> 19550
    println(total(units)) // -> 155
    // Integer literals fit both List<Int> and List<Long>, so an untyped listOf is ambiguous:
    // total(listOf(120, 35))
    //   error: overload resolution ambiguity between candidates:
}
