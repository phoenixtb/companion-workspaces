package com.shelfwise.part12.fromjava

import com.shelfwise.part12.legacy.LegacyProduct

fun main() {
    val product = LegacyProduct.sample() // static methods are called on the class, as in Java

    // A Java single-method interface takes a Kotlin lambda (SAM conversion, Part 5).
    product.onPriceChange { old, new -> println("price $old -> $new") }

    // Getters and setters become properties: this calls setPricePaise(15_900), which fires the listener.
    product.pricePaise = 15_900 // -> price 16500 -> 15900
    product.isActive = false // isActive/setActive become one property, 'isActive'.
    println("${product.sku} ${product.pricePaise} active=${product.isActive}") // -> SHW-1001 15900 active=false

    // getSupplierCode() returns a platform type (String!): Kotlin can't tell if it is nullable (Part 2).
    val code: String? = product.supplierCode // declare the Kotlin type you mean
    println(code ?: "no supplier") // -> no supplier
}
