package com.shelfwise.part07.sequences

import kotlin.streams.asSequence

// The same pipeline twice, for the bytecode comparison.
fun eagerTotal(prices: List<Long>): Long = prices.map { it * 105 / 100 }.filter { it > 20_000 }.sum()

fun lazyTotal(prices: List<Long>): Long = prices.asSequence().map { it * 105 / 100 }.filter { it > 20_000 }.sum()

fun main() {
    val prices = listOf(16_500L, 14_200L, 72_000L, 34_000L, 9_000L)

    // Eager: each step runs over the whole list and builds a new list.
    val trace = mutableListOf<String>()
    val firstEager = prices
        .map { trace += "map $it"; it * 105 / 100 }
        .filter { trace += "filter $it"; it > 20_000 }
        .first()
    println(firstEager) // -> 75600
    println(trace.size) // -> 10

    // Lazy: each element goes through the whole chain; first() stops the pipeline early.
    trace.clear()
    val firstLazy = prices.asSequence()
        .map { trace += "map $it"; it * 105 / 100 }
        .filter { trace += "filter $it"; it > 20_000 }
        .first()
    println(firstLazy) // -> 75600
    println(trace) // -> [map 16500, filter 17325, map 14200, filter 14910, map 72000, filter 75600]

    println(eagerTotal(prices) == lazyTotal(prices)) // -> true

    // Infinite sequences, like Stream.iterate: only what you take is computed.
    val reorderDates = generateSequence(1) { it + 7 }.take(4).toList()
    println(reorderDates) // -> [1, 8, 15, 22]

    // A sequence builder: yield values from ordinary code.
    val labels = sequence {
        yield("HEADER")
        for (aisle in 1..2) yield("AISLE $aisle")
        yield("FOOTER")
    }
    println(labels.joinToString(" / ")) // -> HEADER / AISLE 1 / AISLE 2 / FOOTER

    // Java Streams convert to sequences (and back with asStream()).
    println(java.util.stream.Stream.of("b", "a").asSequence().sorted().toList()) // -> [a, b]
}
