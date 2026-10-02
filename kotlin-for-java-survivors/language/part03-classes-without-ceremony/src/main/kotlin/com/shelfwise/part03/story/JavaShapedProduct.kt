package com.shelfwise.part03.story

// What Kabir typed first: a Java class, transliterated.
class JavaShapedProduct {
    private val name: String
    private var pricePaise: Long

    constructor(name: String, pricePaise: Long) {
        this.name = name
        this.pricePaise = pricePaise
    }

    fun getName(): String = name
    fun getPricePaise(): Long = pricePaise
    fun setPricePaise(pricePaise: Long) {
        this.pricePaise = pricePaise
    }
}

// What Lena typed: the same JVM class, as far as Java callers can tell.
class Product(val name: String, var pricePaise: Long)

fun main() {
    val old = JavaShapedProduct("Toor Dal 1kg", 16_500)
    old.setPricePaise(15_900)
    println(old.getName() + " " + old.getPricePaise()) // -> Toor Dal 1kg 15900

    val new = Product("Toor Dal 1kg", 16_500)
    new.pricePaise = 15_900 // calls setPricePaise under the hood
    println("${new.name} ${new.pricePaise}") // -> Toor Dal 1kg 15900
}
