package com.shelfwise.part01.constants

// const val: a compile-time constant (primitives and String only), inlined at every use site.
const val MAX_LABELS_PER_SHELF = 24

// Plain top-level val: computed when the file's class is initialised, read through a getter.
val PILOT_REGIONS = listOf("Pune", "Bengaluru")

fun main() {
    println(MAX_LABELS_PER_SHELF * 6) // -> 144
    println(PILOT_REGIONS.size) // -> 2
    // const val PILOT = listOf("Pune")  // error: const 'val' has type 'List<String>'. Only primitive types and 'String' are allowed.
}
