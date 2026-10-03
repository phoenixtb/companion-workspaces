package com.shelfwise.part07.report

import com.shelfwise.part07.legacy.LegacyReports
import java.util.Collections

// Step 1: one reading per shelf, and the report the nightly job produces per store.
data class ShelfReading(val store: String, val sku: String, val units: Int, val reorderAt: Int)

data class StoreReport(val store: String, val totalUnits: Int, val lowStock: List<String>)

// Step 2: parse lazily. In production the input is a file with millions of lines (File.useLines);
// a Sequence keeps one line in flight instead of a List of all of them.
fun parse(lines: Sequence<String>): Sequence<ShelfReading> =
    lines
        .filter { it.isNotBlank() && !it.startsWith("#") }
        .map { line ->
            val (store, sku, units, reorderAt) = line.split(",").map { it.trim() }
            ShelfReading(store, sku, units.toInt(), reorderAt.toInt())
        }

// Step 3: aggregate per store as the readings stream past. groupingBy + fold keeps one
// mutable running total per store, not every reading.
private class Totals {
    var units = 0
    val lowStock = mutableListOf<String>()
}

fun report(readings: Sequence<ShelfReading>): List<StoreReport> =
    readings
        .groupingBy { it.store }
        .fold(
            initialValueSelector = { _, _ -> Totals() }, // one accumulator per store
            operation = { _, totals, reading ->
                totals.units += reading.units
                if (reading.units <= reading.reorderAt) totals.lowStock += reading.sku
                totals
            },
        )
        .map { (store, totals) -> StoreReport(store, totals.units, totals.lowStock.sorted().readOnlyForJava()) }
        .sortedBy { it.store }
        .readOnlyForJava()

// Step 4: a read-only list that Java can't modify either.
fun <T> List<T>.readOnlyForJava(): List<T> = Collections.unmodifiableList(this)

// Step 5: run it, publish in batches, and let the Java utility try its luck.
fun main() {
    val export = """
        # store,sku,units,reorderAt
        PUN-014,SHW-1001,4,10
        PUN-014,SHW-2040,35,5
        MUM-002,SHW-1001,60,10
        PUN-014,SHW-3001,0,4
        MUM-002,SHW-3002,2,6
        BLR-007,SHW-5001,300,50
    """.trimIndent()

    val reports = report(parse(export.lineSequence()))
    reports.forEach(::println)
    // -> StoreReport(store=BLR-007, totalUnits=300, lowStock=[])
    // -> StoreReport(store=MUM-002, totalUnits=62, lowStock=[SHW-3002])
    // -> StoreReport(store=PUN-014, totalUnits=39, lowStock=[SHW-1001, SHW-3001])

    // Publish two stores per message (Part 14 sends these to Kafka).
    println(reports.chunked(2).map { batch -> batch.map { it.store } }) // -> [[BLR-007, MUM-002], [PUN-014]]

    try {
        LegacyReports.sortInPlace(reports.last().lowStock)
    } catch (e: UnsupportedOperationException) {
        println("Java can't sort it in place either") // -> Java can't sort it in place either
    }
}
