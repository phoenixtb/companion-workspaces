package com.shelfwise.part11.hot

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest

class ShelfSensor {
    // StateFlow: always has a value; new collectors get the current one; equal updates are skipped.
    private val _units = MutableStateFlow(120)
    val units = _units.asStateFlow() // read-only view for the outside world

    fun scan(newUnits: Int) {
        _units.value = newUnits
    }
}

fun main() = runTest {
    val sensor = ShelfSensor()
    val seen = mutableListOf<Int>()
    val watcher = launch { sensor.units.toList(seen) }
    delay(1)
    for (units in listOf(118, 118, 110, 110, 95)) {
        sensor.scan(units)
        delay(1) // give the watcher time to see every update, so nothing is conflated
    }
    watcher.cancel()
    println(seen) // -> [120, 118, 110, 95]
    // (the repeated 118 and 110 were equal to the current value, so they emitted nothing)

    // Mutating the object inside a StateFlow emits nothing: the reference didn't change.
    val skus = MutableStateFlow(mutableListOf("SHW-1001"))
    var updates = 0
    val counter = launch { skus.collect { updates++ } }
    delay(1)
    skus.value.add("SHW-2040") // invisible to collectors
    delay(1)
    counter.cancel()
    println("updates seen: $updates") // -> updates seen: 1

    // SharedFlow: a broadcast of events. No current value; replay decides what late subscribers get.
    val scans = MutableSharedFlow<String>(replay = 1)
    scans.emit("PUN-014 aisle 7")
    scans.emit("PUN-014 aisle 8")
    val late = scans.take(1).toList()
    println(late) // -> [PUN-014 aisle 8]

    // tryEmit (the non-suspending emit) on a SharedFlow with no buffer: with nobody subscribed,
    // the value is dropped and tryEmit reports success; with a subscriber, it fails.
    val alerts = MutableSharedFlow<String>()
    println(alerts.tryEmit("low stock: SHW-2040")) // -> true
    val listener = launch { alerts.collect { } }
    delay(1)
    println(alerts.tryEmit("low stock: SHW-3001")) // -> false
    listener.cancel()

    // stateIn: one upstream shared by every collector. WhileSubscribed(5_000) starts it with the
    // first subscriber and stops it 5 s (of virtual time, here) after the last one leaves.
    var polls = 0
    val stockPoll = flow {
        polls++ // one database poll loop per start
        while (true) {
            emit(100)
            delay(1_000)
        }
    }
    val shared = stockPoll.stateIn(backgroundScope, SharingStarted.WhileSubscribed(5_000), initialValue = 0)
    val dashboards = List(2) { launch { shared.collect { } } }
    delay(10)
    println("dashboards=2, polls=$polls") // -> dashboards=2, polls=1
    dashboards.forEach { it.cancel() }
    delay(6_000) // nobody subscribed for more than 5 s: the poll stops
    val again = launch { shared.collect { } }
    delay(10)
    println("after a quiet spell, polls=$polls") // -> after a quiet spell, polls=2
    again.cancel()
}
