package com.shelfwise.part12.api

// A value class that validates in init (Part 4). @JvmExposeBoxed (Experimental in 2.4.20) on the class
// adds a public constructor Java can call, and boxed versions of Sku's own members only.
@JvmInline
@JvmExposeBoxed
value class Sku(val value: String) {
    init {
        require(value.matches(Regex("SHW-\\d{4}"))) { "bad SKU: $value" }
    }

    override fun toString() = value
}
