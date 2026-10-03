package com.shelfwise.part11.flows

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking

// An operator is a new flow whose collect collects the upstream. The library's map and filter
// have the same shape; they use an internal builder (unsafeTransform) that skips the safety
// checks flow { } performs on every emit, which this hand-written version keeps.
fun <T, R> Flow<T>.mapped(transform: suspend (T) -> R): Flow<R> = flow {
    collect { value -> emit(transform(value)) }
}

fun readings() = flow {
    println("emit 1"); emit(1)
    println("emit 2"); emit(2)
}

fun main() = runBlocking {
    // emit calls straight down into the collector: each value goes all the way through
    // before the producer gets the next line. One coroutine, one call stack.
    readings().mapped { println("map $it"); it * 10 }.collect { println("collect $it") }
    // -> emit 1
    // -> map 1
    // -> collect 10
    // -> emit 2
    // -> map 2
    // -> collect 20

    // buffer splits the flow in two: everything above it runs in a new producer coroutine,
    // which sends into a channel; the collector receives from it. Now the producer runs ahead.
    readings().mapped { println("map $it"); it * 10 }.buffer().collect { println("collect $it") }
    // -> emit 1
    // -> map 1
    // -> emit 2
    // -> map 2
    // -> collect 10
    // -> collect 20
}
