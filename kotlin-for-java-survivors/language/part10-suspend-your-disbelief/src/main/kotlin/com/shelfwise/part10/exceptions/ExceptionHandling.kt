package com.shelfwise.part10.exceptions

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import java.io.IOException

fun main() = runBlocking {
    // Catching at await() is not enough: the failed child has already failed its parent scope.
    try {
        coroutineScope {
            val failing = async { throw IOException("supplier down") }
            try {
                failing.await()
            } catch (e: IOException) {
                println("await threw: ${e.message}") // -> await threw: supplier down
            }
        }
    } catch (e: IOException) {
        println("scope failed anyway: ${e.message}") // -> scope failed anyway: supplier down
    }

    // In a supervisorScope, a child's failure is only reported through its own await().
    supervisorScope {
        val failing = async { throw IOException("supplier down") }
        try {
            failing.await()
        } catch (e: IOException) {
            println("handled: ${e.message}") // -> handled: supplier down
        }
    }

    // launch has nobody to hand its exception to, so it goes up the job tree. At the root,
    // a CoroutineExceptionHandler is the last resort, the coroutine version of an uncaught-exception handler.
    val handler = CoroutineExceptionHandler { _, e -> println("handler saw: ${e.message}") }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + handler)
    val jobs = listOf(
        scope.launch { throw IOException("index rebuild failed") },
        scope.launch { delay(50); println("other job still ran") },
    )
    jobs.joinAll()
    // -> handler saw: index rebuild failed
    // -> other job still ran
}
