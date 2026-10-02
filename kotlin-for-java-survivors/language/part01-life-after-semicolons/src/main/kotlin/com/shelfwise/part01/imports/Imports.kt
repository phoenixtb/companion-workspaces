package com.shelfwise.part01.imports

// 'as' renames an import, so two classes with the same simple name can live in one file
// without falling back to fully qualified names.
import java.sql.Date as SqlDate
import java.util.Date
import kotlin.math.roundToInt

fun main() {
    val legacy: Date = SqlDate.valueOf("2026-10-01")
    println(legacy) // -> 2026-10-01
    // kotlin.*, kotlin.collections.*, kotlin.text.* and others are imported by default:
    // listOf, println and String need no import. kotlin.math does.
    println((4.6).roundToInt()) // -> 5
}
