package com.shelfwise.part01.ranges

// A range written in the for header compiles to a plain int counter: no IntRange is allocated.
fun totalUnits(perShelf: IntArray): Int {
    var total = 0
    for (i in perShelf.indices) total += perShelf[i]
    return total
}

fun main() {
    println(totalUnits(intArrayOf(12, 0, 7))) // -> 19

    // repeat(n) when you need a count but not an index.
    val chimes = StringBuilder()
    repeat(3) { chimes.append("ding ") }
    println(chimes.trim()) // -> ding ding ding
}
