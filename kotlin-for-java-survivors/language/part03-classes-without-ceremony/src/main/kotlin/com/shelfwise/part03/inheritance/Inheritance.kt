package com.shelfwise.part03.inheritance

// Classes are final unless marked open. So are their members.
open class Promotion(val code: String) {
    open fun discountPaise(pricePaise: Long): Long = 0
    fun describe(): String = "$code: ${discountPaise(10_000)} off ₹100" // final: subclasses can't override
}

class PercentOff(code: String, private val percent: Int) : Promotion(code) {
    // 'override' is mandatory, and an override is itself open unless marked final.
    override fun discountPaise(pricePaise: Long): Long = pricePaise * percent / 100
}

// class Flash : PercentOff("FLASH", 50)  // error: this type is final, so it cannot be extended.

fun main() {
    val promo: Promotion = PercentOff("DIWALI10", percent = 10)
    println(promo.describe()) // -> DIWALI10: 1000 off ₹100
}
