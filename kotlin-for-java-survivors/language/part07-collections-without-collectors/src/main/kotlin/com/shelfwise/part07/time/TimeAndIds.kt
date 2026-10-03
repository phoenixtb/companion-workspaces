package com.shelfwise.part07.time

import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant
import kotlin.time.measureTimedValue
import kotlin.time.toJavaInstant
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid

fun main() {
    // Duration: a value class over a Long, with unit-safe arithmetic.
    val shelfLife: Duration = 36.hours + 30.minutes
    println(shelfLife) // -> 1d 12h 30m
    println(shelfLife.inWholeMinutes) // -> 2190
    println(250.milliseconds < 1.minutes) // -> true

    // measureTimedValue: the result and how long it took, without System.nanoTime() bookkeeping.
    val (total, took) = measureTimedValue { (1..1_000).sum() }
    println("total=$total took=$took") // -> total=500500 took=…

    // kotlin.time.Instant, convertible to java.time at framework boundaries.
    val restock = Instant.parse("2026-10-03T06:00:00Z")
    println(restock + 2.hours) // -> 2026-10-03T08:00:00Z
    println(restock.toJavaInstant().javaClass.name) // -> java.time.Instant

    // kotlin.uuid.Uuid (Stable since 2.4.0), convertible to java.util.UUID.
    val id = Uuid.parse("550e8400-e29b-41d4-a716-446655440000")
    println(id.toJavaUuid().version()) // -> 4
    println(Uuid.random().toString().length) // -> 36
}
