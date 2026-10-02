package com.shelfwise.part01.types

// Inspect with: ./gradlew :language:part01-life-after-semicolons:javap -Pclass=com.shelfwise.part01.types.BoxingKt
fun restock(onShelf: Int, incoming: Int?, history: List<Int>): Int =
    onShelf + (incoming ?: 0) + history.size

fun main() {
    println(restock(onShelf = 12, incoming = null, history = listOf(5, 8))) // -> 14
}
