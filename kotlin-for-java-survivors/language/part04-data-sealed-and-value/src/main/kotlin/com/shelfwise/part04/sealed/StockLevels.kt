package com.shelfwise.part04.sealed

// A closed set of states. Every subtype is known at compile time (same module, same package).
sealed interface StockLevel {
    data class InStock(val units: Int) : StockLevel
    data class Low(val units: Int, val reorderAt: Int) : StockLevel
    data object OutOfStock : StockLevel
    data class Discontinued(val replacementSku: String?) : StockLevel
}

// No else branch: the compiler checks that every subtype is handled.
fun shelfBadge(level: StockLevel): String = when (level) {
    is StockLevel.InStock if level.units > 100 -> "BULK BUY" // guard condition
    is StockLevel.InStock -> ""
    is StockLevel.Low -> "LAST ${level.units}"
    StockLevel.OutOfStock -> "SOLD OUT" // an object is matched by equality, no 'is' needed
    is StockLevel.Discontinued ->
        if (level.replacementSku != null) "TRY ${level.replacementSku}" else "DISCONTINUED"
}

// fun incomplete(level: StockLevel): String = when (level) { is StockLevel.InStock -> "ok"; is StockLevel.Low -> "low"; StockLevel.OutOfStock -> "out" }
//   error: 'when' expression must be exhaustive. Add the 'is Discontinued' branch or an 'else' branch.

// Data-flow exhaustiveness (Stable since 2.3.0): cases ruled out earlier don't need a branch.
fun reorderNote(level: StockLevel): String {
    if (level is StockLevel.OutOfStock || level is StockLevel.Discontinued) return "no action"
    return when (level) {
        is StockLevel.InStock -> "fine"
        is StockLevel.Low -> "reorder at ${level.reorderAt}"
    }
}

fun main() {
    val levels = listOf(
        StockLevel.InStock(240),
        StockLevel.InStock(30),
        StockLevel.Low(4, reorderAt = 10),
        StockLevel.OutOfStock,
        StockLevel.Discontinued(replacementSku = "SHW-2040"),
    )
    for (level in levels) println("$level -> '${shelfBadge(level)}' / ${reorderNote(level)}")
    // -> InStock(units=240) -> 'BULK BUY' / fine
    // -> InStock(units=30) -> '' / fine
    // -> Low(units=4, reorderAt=10) -> 'LAST 4' / reorder at 10
    // -> OutOfStock -> 'SOLD OUT' / no action
    // -> Discontinued(replacementSku=SHW-2040) -> 'TRY SHW-2040' / no action
}
