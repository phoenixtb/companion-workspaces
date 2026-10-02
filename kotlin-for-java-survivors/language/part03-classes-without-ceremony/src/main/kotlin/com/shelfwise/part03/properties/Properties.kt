package com.shelfwise.part03.properties

class ShelfSlot(val aisle: Int, val position: Int, capacity: Int) {
    // A property with an initialiser has a backing field. 'capacity' (no val) is only a constructor parameter.
    var unitsOnShelf: Int = 0
        // A custom setter: 'field' is the backing field. Writing 'unitsOnShelf = value' here would recurse.
        set(value) {
            require(value in 0..maxUnits) { "slot holds 0..$maxUnits units, got $value" }
            field = value
        }

    val maxUnits: Int = capacity

    // A custom getter with no initialiser and no 'field': computed on every read, no backing field at all.
    val code: String
        get() = "A$aisle-P$position"

    val isEmpty: Boolean
        get() = unitsOnShelf == 0

    // Readable everywhere, writable only inside the class.
    var lastRestockedBy: String = "nobody"
        private set

    fun restock(units: Int, by: String) {
        unitsOnShelf += units // goes through the setter, so the range check applies
        lastRestockedBy = by
    }
}

fun main() {
    val slot = ShelfSlot(aisle = 7, position = 3, capacity = 24)
    println(slot.code) // -> A7-P3
    println(slot.isEmpty) // -> true

    slot.restock(20, by = "kabir")
    println("${slot.unitsOnShelf} by ${slot.lastRestockedBy}") // -> 20 by kabir
    // slot.lastRestockedBy = "lena"  // error: cannot access 'lastRestockedBy': it is private in 'com.shelfwise.part03.properties.ShelfSlot'.

    try {
        slot.restock(10, by = "kabir")
    } catch (e: IllegalArgumentException) {
        println(e.message) // -> slot holds 0..24 units, got 30
    }
}
