package com.shelfwise.part04.data

// Validation in init can't be bypassed: copy() calls the constructor, so init runs again.
data class Quantity(val units: Int) {
    init {
        require(units >= 0) { "negative quantity: $units" }
    }
}

// The constructor is private so that every Discount is validated. Without the annotation,
// the generated copy() would stay public and let callers skip the check.
@ConsistentCopyVisibility
data class Discount private constructor(val percent: Int) {
    companion object {
        fun of(percent: Int): Discount {
            require(percent in 1..90) { "discount must be 1..90%, was $percent" }
            return Discount(percent)
        }
    }
}

fun main() {
    try {
        Quantity(5).copy(units = -1)
    } catch (e: IllegalArgumentException) {
        println(e.message) // -> negative quantity: -1
    }

    val festive = Discount.of(10)
    println(festive) // -> Discount(percent=10)
    // festive.copy(percent = 500)  // error: cannot access 'fun copy(percent: Int = ...): Discount': it is private in 'com.shelfwise.part04.data.Discount'.
}
