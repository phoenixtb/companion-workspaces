package com.shelfwise.part09.result

import java.io.IOException
import kotlin.coroutines.cancellation.CancellationException

fun supplierPrice(sku: String): Long = when (sku) {
    "SHW-1001" -> 15_900
    "SHW-9999" -> throw IOException("supplier timeout")
    else -> throw IllegalArgumentException("unknown SKU $sku")
}

fun main() {
    // runCatching turns an exception into a value: Result<T> is either Success or Failure.
    val ok = runCatching { supplierPrice("SHW-1001") }
    val timeout = runCatching { supplierPrice("SHW-9999") }
    println(ok) // -> Success(15900)
    println(timeout) // -> Failure(java.io.IOException: supplier timeout)

    println(ok.map { it / 100 }.getOrThrow()) // -> 159
    println(timeout.getOrElse { 0L }) // -> 0
    println(timeout.fold(onSuccess = { "price $it" }, onFailure = { "failed: ${it.message}" })) // -> failed: supplier timeout

    // Recover only what you expect. recoverCatching keeps a rethrown exception inside the Result;
    // plain recover would let it escape to the caller.
    val recovered = runCatching { supplierPrice("SHW-0000") }
        .recoverCatching { if (it is IOException) -1L else throw it }
    println(recovered.exceptionOrNull()?.message) // -> unknown SKU SHW-0000

    // The trap: runCatching catches Throwable. Cancellation and Errors become ordinary failures.
    println(runCatching { throw CancellationException("job cancelled") }.isFailure) // -> true
    println(runCatching { throw StackOverflowError() }.isFailure) // -> true

    // A narrow catch isn't automatically safe: CancellationException IS an IllegalStateException.
    try {
        throw CancellationException("job cancelled")
    } catch (e: IllegalStateException) {
        println("caught as IllegalStateException: ${e.message}") // -> caught as IllegalStateException: job cancelled
    }
}
