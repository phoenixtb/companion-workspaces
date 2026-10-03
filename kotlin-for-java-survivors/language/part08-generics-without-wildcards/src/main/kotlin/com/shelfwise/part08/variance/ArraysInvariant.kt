package com.shelfwise.part08.variance

// Java arrays are covariant: String[] is an Object[], and a wrong write fails at runtime.
// Kotlin arrays are invariant; use-site 'out' gives the read-only flexibility.
fun printAll(items: Array<out Any>) = println(items.joinToString())

fun main() {
    val skus: Array<String> = arrayOf("SHW-1001", "SHW-2040")
    // val anything: Array<Any> = skus  // error: initializer type mismatch: expected 'Array<Any>', actual 'Array<String>'.
    printAll(skus) // -> SHW-1001, SHW-2040

    // The Java behaviour, through a Java array type: compiles, then fails at runtime.
    val javaStyle: Array<Any> = java.lang.reflect.Array.newInstance(String::class.java, 1) as Array<Any>
    try {
        javaStyle[0] = 42
    } catch (e: ArrayStoreException) {
        println("ArrayStoreException: ${e.message}") // -> ArrayStoreException: java.lang.Integer
    }
}
