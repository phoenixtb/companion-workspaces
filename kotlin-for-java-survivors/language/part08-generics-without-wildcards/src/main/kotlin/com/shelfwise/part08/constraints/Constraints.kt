package com.shelfwise.part08.constraints

// Upper bound, like <T extends Comparable<T>>.
fun <T : Comparable<T>> maxOf3(a: T, b: T, c: T): T = maxOf(a, maxOf(b, c))

// Several bounds need 'where', like <T extends CharSequence & Comparable<T>>.
fun <T> longestSorted(items: List<T>): T? where T : CharSequence, T : Comparable<T> =
    items.sorted().maxByOrNull { it.length }

// The default bound is Any?, not Object: an unbounded T accepts null.
fun <T> describe(value: T): String = "value=$value"

// T : Any rules nulls out.
fun <T : Any> describeNonNull(value: T): String = "value=$value"

// Definitely non-null type T & Any: "T, but never null", for a T that may itself be nullable.
fun <T> orDefault(value: T, default: T & Any): T & Any = value ?: default

fun main() {
    println(maxOf3(16_500, 72_000, 34_000)) // -> 72000
    println(longestSorted(listOf("dal", "basmati", "ghee"))) // -> basmati
    println(describe(null)) // -> value=null
    // describeNonNull(null)  // error: null cannot be a value of a non-null type 'uninferred T (of fun <T : Any> describeNonNull)'.
    val maybeName: String? = null
    println(orDefault(maybeName, "unknown").length) // -> 7
}
