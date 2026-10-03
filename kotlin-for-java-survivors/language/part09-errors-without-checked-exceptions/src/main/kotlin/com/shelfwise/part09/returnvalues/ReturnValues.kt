package com.shelfwise.part09.returnvalues

// Experimental in 2.4.20 — requires -Xreturn-value-checker=check (set in this module's build.gradle.kts).
// Every function in a @MustUseReturnValues class must have its result used.
@MustUseReturnValues
class PriceCalculator {
    fun discounted(pricePaise: Long, percent: Int): Long = pricePaise * (100 - percent) / 100

    // Opt a function back out: callers may ignore the result.
    @IgnorableReturnValue
    fun record(pricePaise: Long): Boolean = pricePaise > 0
}

fun main() {
    val calculator = PriceCalculator()
    var price = 16_500L
    calculator.discounted(price, 10) // warning: unused return value of 'discounted'.
    println(price) // -> 16500

    price = calculator.discounted(price, 10)
    println(price) // -> 14850

    calculator.record(price) // fine: @IgnorableReturnValue
    val _ = calculator.discounted(price, 5) // an explicit "I mean to ignore this"
}
