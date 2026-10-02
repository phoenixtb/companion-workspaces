package com.shelfwise.part03.nested

class Aisle(val number: Int) {
    private val slots = mutableListOf<String>()

    // Nested by default: no reference to an Aisle. This is Java's *static* nested class.
    class Builder {
        private var number = 0
        fun number(value: Int) = apply { number = value }
        fun build() = Aisle(number)
        // fun slotCount() = slots.size  // error: outer class 'class Aisle : Any' of non-inner class cannot be used as receiver.
    }

    // 'inner' captures the enclosing Aisle, like a Java inner class without 'static'.
    inner class Slot(val position: Int) {
        fun label(): String = "A$number-P$position" // reads the outer instance's 'number'
        fun fill(product: String) {
            slots += product // and can touch its private state
        }
    }

    fun filled(): List<String> = slots
}

fun main() {
    val aisle = Aisle.Builder().number(7).build() // no Aisle instance needed to create a Builder
    val slot = aisle.Slot(3) // an inner class needs one
    slot.fill("Basmati Rice 5kg")
    println(slot.label()) // -> A7-P3
    println(aisle.filled()) // -> [Basmati Rice 5kg]
}
