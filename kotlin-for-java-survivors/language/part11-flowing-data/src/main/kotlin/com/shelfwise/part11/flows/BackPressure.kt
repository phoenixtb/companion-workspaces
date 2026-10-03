package com.shelfwise.part11.flows

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest

// A scanner that emits a reading every 100 ms (of virtual time: runTest skips the waiting).
// Each consumer below takes 300 ms per reading.
fun scans(count: Int) = flow {
    repeat(count) {
        delay(100)
        emit(it)
    }
}

// Runs a block and prints what it collected and how much virtual time it took.
@OptIn(ExperimentalCoroutinesApi::class) // currentTime is experimental in kotlinx-coroutines-test 1.11.0
suspend fun TestScope.timed(label: String, block: suspend () -> List<Int>) {
    val start = currentTime
    val seen = block()
    println("$label: saw $seen in ${currentTime - start} ms")
}

@OptIn(FlowPreview::class) // debounce is a Flow preview API in kotlinx.coroutines 1.11.0
fun main() = runTest {
    // No buffer: producer and consumer take turns, 100 + 300 ms per reading.
    timed("sequential") { buildList { scans(5).collect { delay(300); add(it) } } }
    // -> sequential: saw [0, 1, 2, 3, 4] in 2000 ms

    // buffer: the producer runs ahead in its own coroutine; the consumer sets the pace.
    timed("buffer") { buildList { scans(5).buffer().collect { delay(300); add(it) } } }
    // -> buffer: saw [0, 1, 2, 3, 4] in 1600 ms

    // conflate: the consumer only ever sees the latest value; intermediate ones are dropped.
    timed("conflate") { buildList { scans(5).conflate().collect { delay(300); add(it) } } }
    // -> conflate: saw [0, 2, 4] in 1000 ms

    // collectLatest: a new value cancels the work on the previous one.
    timed("collectLatest") { buildList { scans(5).collectLatest { delay(300); add(it) } } }
    // -> collectLatest: saw [4] in 800 ms

    // debounce: emit only after 250 ms of quiet. A burst of scans becomes one update.
    val burst = flow {
        emit(1); delay(50); emit(2); delay(50); emit(3) // a burst
        delay(400)
        emit(4) // a lone scan
    }
    timed("debounce") { burst.debounce(250).toList() }
    // -> debounce: saw [3, 4] in 500 ms
    // (the last value goes out as soon as the flow completes, without waiting 250 ms)
}
