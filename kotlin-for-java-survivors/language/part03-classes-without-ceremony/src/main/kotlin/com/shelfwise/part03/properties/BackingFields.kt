package com.shelfwise.part03.properties

// The old pattern: a private mutable property and a public read-only view of it.
class PriceHistoryOld {
    private val _changes = mutableListOf<Long>()
    val changes: List<Long> get() = _changes

    fun record(pricePaise: Long) {
        _changes += pricePaise
    }
}

// Explicit backing field (Stable in Kotlin 2.4.0): one property, two types.
// Outside the class it is a List<Long>; inside, the compiler knows the field is a MutableList.
class PriceHistory {
    val changes: List<Long>
        field = mutableListOf()

    fun record(pricePaise: Long) {
        changes += pricePaise // smart-cast to MutableList<Long> inside the class
    }
}

fun main() {
    val history = PriceHistory()
    history.record(16_500)
    history.record(15_900)
    println(history.changes) // -> [16500, 15900]
    // Outside the class, += on a read-only List means 'changes = changes + 1', which needs a var:
    // history.changes += 1  // error: 'val' cannot be reassigned.
    // The read-only view is a compile-time guarantee only: the object really is a MutableList.
    println(history.changes is MutableList<*>) // -> true
}
