package com.shelfwise.part04.destructuring

data class ShelfReading(val sku: String, val units: Int, val aisle: Int)

fun main() {
    val reading = ShelfReading("SHW-1001", 12, 7)

    // Positional: the names on the left are yours; only the order matters.
    val (code, onShelf) = reading
    println("$code: $onShelf") // -> SHW-1001: 12
    val (_, _, aisle) = reading // '_' skips a component without calling it
    println(aisle) // -> 7

    // The trap: names that look meaningful are ignored. This compiles and swaps the values.
    val (units, sku) = reading
    println("units=$units sku=$sku") // -> units=SHW-1001 sku=12

    // Destructuring in for loops and lambdas: Map.Entry and Pair have component1/component2 too.
    val stock = mapOf("SHW-1001" to 12, "SHW-1002" to 0)
    for ((item, count) in stock) println("$item=$count")
    // -> SHW-1001=12
    // -> SHW-1002=0
    println(stock.filter { (_, count) -> count == 0 }.keys) // -> [SHW-1002]

    // Experimental in 2.4.20 — requires -Xname-based-destructuring=only-syntax.
    // Each variable is bound to a property by name, so order no longer matters.
    (val shelfUnits = units, val shelfSku = sku) = reading
    println("$shelfSku: $shelfUnits") // -> SHW-1001: 12
}
