package com.shelfwise.part01.ranges

fun main() {
    // 1..5 includes both ends; 0..<5 excludes the end (the Java for-loop shape).
    println((1..5).toList()) // -> [1, 2, 3, 4, 5]
    println((0..<5).toList()) // -> [0, 1, 2, 3, 4]

    // Counting down needs downTo: a range whose start is above its end is simply empty.
    println((5..1).toList()) // -> []
    println((10 downTo 0 step 5).toList()) // -> [10, 5, 0]

    // 'in' works on any range, including chars, and inside if and when.
    println(7 in 1..10) // -> true
    println('q' !in 'a'..'m') // -> true

    val aisles = listOf("Dairy", "Bakery", "Frozen")
    val numbered = mutableListOf<String>()
    for ((index, aisle) in aisles.withIndex()) {
        numbered += "${index + 1}:$aisle"
    }
    println(numbered) // -> [1:Dairy, 2:Bakery, 3:Frozen]
}
