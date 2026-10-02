package com.shelfwise.part01.types

fun main() {
    val unitsSold: Int = 1_250
    val paisePerUnit = 19_900L // Long literal

    // No implicit widening, not even Int to Long:
    // val revenue: Long = unitsSold  // error: initializer type mismatch: expected 'Long', actual 'Int'.
    val revenue: Long = unitsSold.toLong() * paisePerUnit
    println(revenue) // -> 24875000

    // Arithmetic across types is fine: Int.times(Long) is an overload that returns Long.
    println(unitsSold * paisePerUnit) // -> 24875000

    // Equality across types is not:
    // println(unitsSold == 1_250L)  // error: operator '==' cannot be applied to 'Int' and 'Long'.
    println(unitsSold.toLong() == 1_250L) // -> true

    // Char is not a number.
    val grade = 'A'
    // val code: Int = grade  // error: initializer type mismatch: expected 'Int', actual 'Char'.
    println(grade.code) // -> 65
    println(grade + 1) // -> B

    // The JVM is still underneath: same overflow, same integer division.
    println(Int.MAX_VALUE + 1) // -> -2147483648
    println(7 / 2) // -> 3
}
