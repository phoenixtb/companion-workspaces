package com.shelfwise.part10.traps

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

// Stands in for SLF4J's MDC or Spring Security's SecurityContextHolder: state kept per thread.
val currentStore = ThreadLocal<String>()

suspend fun logLine(message: String): String = withContext(Dispatchers.Default) {
    delay(1) // a suspension point; the code may resume on any worker thread
    "[store=${currentStore.get()}] $message"
}

private val lock = Any()

fun main() = runBlocking {
    // 1. A ThreadLocal set on one thread is not there after the coroutine moves threads.
    currentStore.set("PUN-014")
    println(logLine("price updated")) // -> [store=null] price updated
    // asContextElement makes the coroutine carry the value and install it on whichever thread runs it.
    withContext(currentStore.asContextElement("PUN-014")) {
        println(logLine("price updated")) // -> [store=PUN-014] price updated
    }
    currentStore.remove()

    // 2. catch (e: Exception) catches CancellationException too, so this loop ignores cancellation.
    var rounds = 0
    val stubborn = launch {
        repeat(5) {
            try {
                delay(20)
            } catch (e: Exception) {
                // a Java habit: log and carry on. Here it swallows the cancellation.
            }
            rounds++
        }
    }
    delay(30)
    stubborn.cancel()
    stubborn.join()
    println("rounds completed after cancel: $rounds") // -> rounds completed after cancel: 5

    // 3. A lock held across a suspension point would be released on another thread, so it doesn't compile.
    synchronized(lock) {
        // delay(10)  // error: the 'delay' suspension point is inside a critical section.
        println("synchronized blocks can't suspend") // -> synchronized blocks can't suspend
    }
}
