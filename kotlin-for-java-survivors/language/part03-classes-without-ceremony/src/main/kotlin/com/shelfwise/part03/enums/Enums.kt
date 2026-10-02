package com.shelfwise.part03.enums

enum class StorageZone(val minCelsius: Int, val maxCelsius: Int) {
    AMBIENT(15, 30),
    CHILLED(0, 5) {
        override fun handlingNote() = "keep the cold chain unbroken"
    },
    FROZEN(-25, -18) {
        override fun handlingNote() = "restock within 20 minutes"
    };

    // An open member with a default; constants with a body override it.
    open fun handlingNote(): String = "no special handling"

    fun accepts(celsius: Int): Boolean = celsius in minCelsius..maxCelsius
}

fun main() {
    // entries: a fixed, cached List. values() still exists, but allocates a new array on every call.
    println(StorageZone.entries.map { it.name }) // -> [AMBIENT, CHILLED, FROZEN]
    println(StorageZone.entries.first { it.accepts(-20) }) // -> FROZEN
    println(StorageZone.CHILLED.handlingNote()) // -> keep the cold chain unbroken
    println(StorageZone.AMBIENT.handlingNote()) // -> no special handling
    println(StorageZone.valueOf("CHILLED").ordinal) // -> 1
    println(StorageZone.values() === StorageZone.values()) // -> false
    println(StorageZone.entries === StorageZone.entries) // -> true
}
