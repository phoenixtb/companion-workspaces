package com.shelfwise.part07.story

import com.shelfwise.part07.legacy.LegacyReports

class Assortment {
    private val skus = mutableListOf("SHW-2040", "SHW-1001", "SHW-3001")

    // Read-only to Kotlin callers. To Java, it is the same java.util.List, set() and all.
    fun skus(): List<String> = skus

    // The fix: hand out a copy, exactly as you would in Java.
    fun skusSnapshot(): List<String> = skus.toList()
}

fun main() {
    val assortment = Assortment()
    LegacyReports.sortInPlace(assortment.skus())
    println(assortment.skus()) // -> [SHW-1001, SHW-2040, SHW-3001]

    val fresh = Assortment()
    LegacyReports.sortInPlace(fresh.skusSnapshot())
    println(fresh.skus()) // -> [SHW-2040, SHW-1001, SHW-3001]
}
