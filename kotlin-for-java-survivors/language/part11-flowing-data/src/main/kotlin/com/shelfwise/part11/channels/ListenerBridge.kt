package com.shelfwise.part11.channels

import kotlinx.coroutines.async
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield

data class Scan(val sku: String, val units: Int)

// A Java listener API as a Flow. callbackFlow gives the block a channel: callbacks push into it,
// the collector reads from it, and awaitClose keeps the listener registered until the collector stops.
fun ScannerSdk.scans(): Flow<Scan> = callbackFlow {
    val listener = object : ScannerSdk.ScanListener {
        override fun onScan(sku: String, units: Int) {
            trySend(Scan(sku, units)) // callbacks can't suspend, so the non-suspending send
        }

        override fun onError(e: Exception) {
            close(e) // fails the flow with the SDK's exception
        }
    }
    addListener(listener)
    awaitClose { removeListener(listener) } // runs on completion, failure or cancellation
}

fun main() = runBlocking {
    val sdk = ScannerSdk()
    val firstTwo = async { sdk.scans().take(2).toList() }
    while (sdk.listenerCount() == 0) yield() // let the collector start and register

    sdk.deliver("SHW-1001", 40)
    sdk.deliver("SHW-2040", 8)
    sdk.deliver("SHW-3001", 75) // buffered, then discarded when take(2) cancels the flow

    println(firstTwo.await()) // -> [Scan(sku=SHW-1001, units=40), Scan(sku=SHW-2040, units=8)]
    println("listeners after: ${sdk.listenerCount()}") // -> listeners after: 0
}
