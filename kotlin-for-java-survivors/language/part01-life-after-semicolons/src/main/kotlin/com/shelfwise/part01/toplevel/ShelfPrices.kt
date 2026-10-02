@file:JvmName("ShelfPrices") // Java callers see ShelfPrices.perKilo(...), not ShelfPricesKt
package com.shelfwise.part01.toplevel

fun perKilo(paise: Long, grams: Int): Long = paise * 1_000 / grams

fun main() {
    println(perKilo(paise = 8_990, grams = 500)) // -> 17980
}
