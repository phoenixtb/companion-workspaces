package com.shelfwise.part05.returns

// forEach is inline, so this 'return' leaves firstPremium, not just the lambda.
fun firstPremium(prices: List<Long>): Long? {
    prices.forEach { if (it >= 50_000) return it }
    return null
}

// An anonymous function: 'return' leaves the anonymous function itself, like a Java lambda.
fun countAffordable(prices: List<Long>, budgetPaise: Long): Int {
    var count = 0
    prices.forEach(fun(price) {
        if (price > budgetPaise) return
        count++
    })
    return count
}

// Not inline: the lambda may be stored and called later, so a bare 'return' has no caller to leave.
fun eachLater(prices: List<Long>, action: (Long) -> Unit) {
    for (price in prices) action(price)
}

fun printNonZero(prices: List<Long>) {
    eachLater(prices) { if (it == 0L) return@eachLater else println("price $it") }
    // eachLater(prices) { if (it == 0L) return }  // error: 'return' is prohibited here.
    // Sequence operations are not inline either, because they run later:
    // prices.asSequence().map { if (it == 0L) return else it }.toList()  // error: 'return' is prohibited here.
}

// Non-local break and continue (Stable since 2.2.0) work inside inline lambdas too.
fun firstStockedShelf(shelves: List<List<Int>>): Int {
    var found = -1
    for ((index, shelf) in shelves.withIndex()) {
        shelf.forEach { units ->
            if (units < 0) continue // a faulty sensor: skip the rest of this shelf
            if (units > 0) {
                found = index
                break // stop scanning shelves altogether
            }
        }
    }
    return found
}

fun main() {
    println(firstPremium(listOf(16_500, 72_000, 99_000))) // -> 72000
    println(countAffordable(listOf(16_500, 72_000, 9_900), budgetPaise = 20_000)) // -> 2
    printNonZero(listOf(0, 16_500))
    // -> price 16500
    println(firstStockedShelf(listOf(listOf(0, 0), listOf(-1, 5), listOf(0, 3)))) // -> 2
}
