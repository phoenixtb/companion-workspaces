package com.shelfwise.part03.constructors

open class Shelf(val code: String) {
    init {
        // Calling an open member from a constructor: the subclass isn't initialised yet.
        println("registering ${describe()}")
    }

    open fun describe(): String = "shelf $code"
}

class ChilledShelf(code: String, val celsius: Int) : Shelf(code) {
    // A non-null String, which is still null while Shelf's init block runs.
    val label: String = "chilled at $celsius°C"

    override fun describe(): String = "$code: $label"
}

fun main() {
    val shelf = ChilledShelf("D-04", celsius = 4)
    // -> registering D-04: null
    println(shelf.describe()) // -> D-04: chilled at 4°C
}
