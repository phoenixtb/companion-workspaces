package com.shelfwise.part11.dashboard

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import reactor.core.publisher.Flux

// Step 1: a scan from a shelf scanner.
data class ShelfScan(val store: String, val sku: String, val units: Int)

// Step 2: the board holds state: the latest units per "store/sku", as an immutable map.
class StockBoard(private val reorderAt: Int = 10) {
    private val _levels = MutableStateFlow<Map<String, Int>>(emptyMap())
    val levels: StateFlow<Map<String, Int>> = _levels.asStateFlow()

    // Atomic read-modify-write: safe when scans arrive from many coroutines (or threads) at once.
    fun record(scan: ShelfScan) = _levels.update { it + ("${scan.store}/${scan.sku}" to scan.units) }

    // Step 3: alerts derived from the state, emitted only when the low-stock set changes.
    val lowStock: Flow<Set<String>> = levels
        .map { all -> all.filterValues { it <= reorderAt }.keys }
        .distinctUntilChanged()
}

// Step 4: the same stream as a Flux, for Reactor-based callers.
fun StockBoard.lowStockFeed(): Flux<Set<String>> = lowStock.asFlux()

// Step 5: drive it under virtual time.
@OptIn(ExperimentalCoroutinesApi::class) // runCurrent is experimental in kotlinx-coroutines-test 1.11.0
fun main() = runTest {
    val board = StockBoard()
    val alerts = mutableListOf<Set<String>>()
    backgroundScope.launch { board.lowStock.toList(alerts) }

    for (scan in listOf(
        ShelfScan("PUN-014", "SHW-1001", 40),
        ShelfScan("PUN-014", "SHW-2040", 8),
        ShelfScan("MUM-002", "SHW-1001", 5),
        ShelfScan("PUN-014", "SHW-2040", 60),
    )) {
        board.record(scan)
        runCurrent() // let the collector see each state; a StateFlow keeps only the latest
    }

    println(board.levels.value) // -> {PUN-014/SHW-1001=40, PUN-014/SHW-2040=60, MUM-002/SHW-1001=5}
    alerts.forEach(::println)
    // -> []
    // -> [PUN-014/SHW-2040]
    // -> [PUN-014/SHW-2040, MUM-002/SHW-1001]
    // -> [MUM-002/SHW-1001]
}
