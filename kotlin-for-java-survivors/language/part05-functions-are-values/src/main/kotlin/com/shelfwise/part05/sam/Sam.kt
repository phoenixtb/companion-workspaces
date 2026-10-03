package com.shelfwise.part05.sam

import com.shelfwise.part05.javarules.PriceCheck
import java.util.concurrent.Callable
import java.util.concurrent.Executors

// A Kotlin interface needs 'fun' to accept a lambda. That marks it as a single abstract method type.
fun interface Discount {
    fun discounted(pricePaise: Long): Long
}

interface PlainDiscount {
    fun discounted(pricePaise: Long): Long
}

fun countAccepted(prices: List<Long>, check: PriceCheck): Int = prices.count { check.accepts(it) }

fun main() {
    // SAM conversion for a Java interface: a lambda where Java expects PriceCheck.
    println(countAccepted(listOf(500, 20_000, 90_000)) { it in 1_000..50_000 }) // -> 1

    // fun interface: a lambda converts to it too.
    val festive: Discount = Discount { it * 90 / 100 }
    val none: Discount = { it } // the explicit Discount { } is optional when the type is known
    println(festive.discounted(10_000) + none.discounted(1)) // -> 9001

    // A plain Kotlin interface doesn't take a lambda; you need an object expression.
    // val plain: PlainDiscount = { it }  // error: initializer type mismatch: expected 'PlainDiscount', actual '() -> ??? (Unknown lambda return type)'.
    val plain = object : PlainDiscount {
        override fun discounted(pricePaise: Long): Long = pricePaise
    }
    println(plain.discounted(42)) // -> 42

    // JDK overloads: Kotlin and Java pick different ones for the same-looking lambda.
    val pool = Executors.newSingleThreadExecutor()
    println(pool.submit { "reindexed" }.get()) // -> null
    println(pool.submit(Callable { "reindexed" }).get()) // -> reindexed
    pool.shutdown()
}
