package com.shelfwise.part08.extensions

data class Product(val sku: String, val pricePaise: Long)

// An extension on one parameterisation: Java can't add a method to List<Product> only.
fun List<Product>.totalPaise(): Long = sumOf { it.pricePaise }

// A bounded generic receiver: available on any List<T> whose T is Comparable.
fun <T : Comparable<T>> List<T>.isAscending(): Boolean = zipWithNext().all { (a, b) -> a <= b }

fun main() {
    val basket = listOf(Product("SHW-1001", 16_500), Product("SHW-2040", 72_000))
    println(basket.totalPaise()) // -> 88500
    println(listOf("SHW-1001", "SHW-2040").isAscending()) // -> true
    // listOf(Product("SHW-1001", 1)).isAscending()
    //   error: candidate 'fun <T : Comparable<T>> List<T>.isAscending(): Boolean' is inapplicable because of a receiver type mismatch.

    // Nothing + out: a List<Nothing> is a List<Product>, because Nothing is a subtype of every type.
    val nothing: List<Nothing> = listOf()
    val none: List<Product> = nothing
    println(none.totalPaise()) // -> 0
}
