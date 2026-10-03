package com.shelfwise.part04.data

data class Basket(val items: Array<String>) // arrays keep identity-based equals

data class StockKey(var sku: String) // a var in a data class used as a hash key

fun main() {
    println(Basket(arrayOf("rice")) == Basket(arrayOf("rice"))) // -> false

    val key = StockKey("SHW-1001")
    val stock = hashMapOf(key to 12)
    key.sku = "SHW-9999" // the hashCode changes; the entry is now in the wrong bucket
    println(stock[key]) // -> null
    println(stock[StockKey("SHW-1001")]) // -> null
    println(stock.size) // -> 1
}
