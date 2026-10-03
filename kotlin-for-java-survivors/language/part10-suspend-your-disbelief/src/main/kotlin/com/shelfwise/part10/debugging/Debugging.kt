package com.shelfwise.part10.debugging

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import java.io.IOException

suspend fun supplierQuote(sku: String): Long {
    delay(10)
    throw IOException("no quote for $sku")
}

// Run with -Dkotlinx.coroutines.debug (this module's tests set it): coroutine names appear in thread names,
// and exceptions rethrown across coroutines get the awaiting coroutine's frames added back.
fun main(): Unit = runBlocking(CoroutineName("nightly-import")) {
    println(Thread.currentThread().name) // -> … @nightly-import#…

    supervisorScope {
        val quote = async(CoroutineName("quote-SHW-1001")) { supplierQuote("SHW-1001") }
        try {
            quote.await()
        } catch (e: IOException) {
            e.stackTraceToString().lines().take(5).forEach(::println)
            // -> java.io.IOException: no quote for SHW-1001
            // -> 	at com.shelfwise.part10.debugging.DebuggingKt.supplierQuote(Debugging.kt:12)
            // -> 	at com.shelfwise.part10.debugging.DebuggingKt$supplierQuote$1.invokeSuspend(Debugging.kt)
            // -> 	at _COROUTINE._BOUNDARY._(CoroutineDebugging.kt:42)
            // -> 	at com.shelfwise.part10.debugging.DebuggingKt$main$1$1.invokeSuspend(Debugging.kt:23)
        }
    }
}
