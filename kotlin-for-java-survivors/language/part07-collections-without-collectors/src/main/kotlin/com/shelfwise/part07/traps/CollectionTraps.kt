package com.shelfwise.part07.traps

class Shelf {
    val skus = mutableListOf<String>()
}

fun main() {
    // remove(1) on a MutableList<Int> removes the element 1. Java's remove(int) removes index 1.
    val units = mutableListOf(5, 1, 7)
    units.remove(1)
    println(units) // -> [5, 7]
    units.removeAt(1)
    println(units) // -> [5]

    // reversed() is a copy. Java 21's List.reversed() is a live view; Kotlin's extension wins.
    val aisle = mutableListOf("A1", "A2")
    val backwards = aisle.reversed()
    aisle += "A3"
    println(backwards) // -> [A2, A1]

    // Grouping.fold(initialValue) shares ONE initial object between all keys.
    val readings = listOf("PUN" to "SHW-1001", "MUM" to "SHW-2040", "PUN" to "SHW-3001")
    val shared = readings.groupingBy { it.first }.fold(Shelf()) { shelf, (_, sku) -> shelf.apply { skus += sku } }
    println(shared.mapValues { it.value.skus }) // -> {PUN=[SHW-1001, SHW-2040, SHW-3001], MUM=[SHW-1001, SHW-2040, SHW-3001]}

    // initialValueSelector creates one accumulator per key, like a Collector's supplier.
    val perStore = readings.groupingBy { it.first }.fold(
        initialValueSelector = { _, _ -> Shelf() },
        operation = { _, shelf, (_, sku) -> shelf.apply { skus += sku } },
    )
    println(perStore.mapValues { it.value.skus }) // -> {PUN=[SHW-1001, SHW-3001], MUM=[SHW-2040]}
}
