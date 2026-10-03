package com.shelfwise.part07.arrays

fun totalUnits(vararg units: Int): Int = units.sum() // vararg Int arrives as an IntArray (int[])

fun main() {
    val boxed: Array<Int> = arrayOf(120, 0, 35) // Integer[] in bytecode
    val primitive: IntArray = intArrayOf(120, 0, 35) // int[] in bytecode
    println(boxed.javaClass.simpleName + " / " + primitive.javaClass.simpleName) // -> Integer[] / int[]

    // Arrays are Java arrays: identity equals and an unhelpful toString.
    println(primitive.toString().startsWith("[I@")) // -> true
    println(primitive.contentToString()) // -> [120, 0, 35]
    println(intArrayOf(1, 2) == intArrayOf(1, 2)) // -> false
    println(intArrayOf(1, 2) contentEquals intArrayOf(1, 2)) // -> true

    // The collection operations work on arrays too, and return Lists.
    println(primitive.filter { it > 0 }) // -> [120, 35]

    // Spread operator: pass an array where varargs are expected.
    println(totalUnits(*primitive, 5)) // -> 160

    // Sized constructor with an initializer.
    println(IntArray(3) { it * 10 }.contentToString()) // -> [0, 10, 20]
}
