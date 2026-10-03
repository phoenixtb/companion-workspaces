package com.shelfwise.part11.channels

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap

// A channel is a queue between coroutines: each element goes to exactly ONE receiver.
// That makes it the tool for distributing work, where a Flow is the tool for describing a stream.
@OptIn(ExperimentalCoroutinesApi::class) // produce { } is experimental in kotlinx.coroutines 1.11.0
fun main() = runBlocking {
    val labelsPrinted = ConcurrentHashMap<String, Int>()

    coroutineScope {
        val jobs = produce(capacity = Channel.BUFFERED) {
            for (aisle in 1..12) send("aisle-$aisle")
        } // closed automatically when the producer finishes

        repeat(3) { printer ->
            launch {
                for (job in jobs) { // fan-out: three printers share the queue
                    delay(10)
                    labelsPrinted.merge("printer-$printer", 1, Int::plus)
                }
            }
        }
    }
    println("${labelsPrinted.values.sum()} labels, ${labelsPrinted.size} printers busy") // -> 12 labels, 3 printers busy
}
