package com.shelfwise.part07.readonly

fun main() {
    val shelf = mutableListOf("SHW-1001", "SHW-2040")
    val view: List<String> = shelf // the same object, seen through a narrower interface

    // view.add("SHW-3001")  // error: unresolved reference 'add'.
    shelf += "SHW-3001"
    println(view) // -> [SHW-1001, SHW-2040, SHW-3001]

    // What the factories return at runtime: ordinary JDK collections, mostly.
    println(listOf("a", "b")::class.java.name) // -> java.util.Arrays$ArrayList
    println(listOf("a")::class.java.name) // -> java.util.Collections$SingletonList
    println(mutableListOf("a")::class.java.name) // -> java.util.ArrayList
    println(listOf("b", "a").map { it }::class.java.name) // -> java.util.ArrayList
    println(mapOf("a" to 1, "b" to 2)::class.java.name) // -> java.util.LinkedHashMap
    println(setOf("a", "b")::class.java.name) // -> java.util.LinkedHashSet

    // A cast defeats the read-only interface, because the object underneath is mutable.
    val mapped: List<String> = listOf("b", "a").map { it.uppercase() }
    (mapped as MutableList<String>).add("C")
    println(mapped) // -> [B, A, C]

    // buildList is the exception: its result is read-only at runtime too.
    val built: List<String> = buildList { add("x") }
    try {
        (built as MutableList<String>).add("y")
    } catch (e: UnsupportedOperationException) {
        println("buildList result is read-only") // -> buildList result is read-only
    }
}
