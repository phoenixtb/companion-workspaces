package com.shelfwise.part03.inheritance

// Interfaces can declare properties (abstract, or with a getter) and default methods.
interface Priced {
    val pricePaise: Long
    fun priceLabel(): String = "₹${pricePaise / 100}"
}

interface Labelled {
    val name: String
    val shelfName: String get() = name.uppercase() // a property with a default getter, no field
    fun priceLabel(): String = "see shelf"
}

// Both interfaces supply priceLabel(), so the class must override it,
// and super<T> picks which default to call. Same rule as Java's Priced.super.priceLabel().
class Item(override val name: String, override val pricePaise: Long) : Priced, Labelled {
    override fun priceLabel(): String = "${super<Labelled>.priceLabel()} — ${super<Priced>.priceLabel()}"
}

// class Broken(override val name: String, override val pricePaise: Long) : Priced, Labelled
//   error: class 'Broken' must override 'priceLabel' because it inherits multiple interface methods for it.

fun main() {
    val ghee = Item("Desi Ghee 1L", 64_900)
    println(ghee.shelfName) // -> DESI GHEE 1L
    println(ghee.priceLabel()) // -> see shelf — ₹649
}
