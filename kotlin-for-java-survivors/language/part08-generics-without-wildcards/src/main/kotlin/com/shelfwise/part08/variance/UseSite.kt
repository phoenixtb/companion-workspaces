package com.shelfwise.part08.variance

// Use-site projections: Java's ? extends / ? super, for types that are invariant by declaration.
fun copyAll(from: MutableList<out Product>, to: MutableList<in Product>) {
    for (item in from) to.add(item) // read from 'out', write to 'in'
    // from.add(Product("SHW-0000"))  // error: receiver type 'MutableList<out Product>' contains out projection which prohibits the use of 'fun add(element: E): Boolean'.
}

// Star projection: "some type I don't know". Safe to read (as Any?), not to write.
fun describe(items: MutableList<*>): String = "${items.size} items, first is ${items.firstOrNull()}"
// fun addTo(items: MutableList<*>) = items.add("x")  // error: receiver type 'MutableList<*>' contains star projection which prohibits the use of 'fun add(element: E): Boolean'.

fun main() {
    val dairy = mutableListOf(PerishableProduct("SHW-3002", 5))
    val everything = mutableListOf<Any>("header")
    copyAll(dairy, everything)
    println(everything.size) // -> 2
    println(describe(mutableListOf(1, 2, 3))) // -> 3 items, first is 1
}
