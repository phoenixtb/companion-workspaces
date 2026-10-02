package com.shelfwise.part02.platform

import com.supplier.sdk.SupplierItem
import com.supplier.sdk.v2.SupplierItemV2

fun main() {
    val item = SupplierItem("SUP-7781", null, listOf("gluten"))

    // item.description is a platform type, shown by the IDE as String!.
    // Kotlin lets you treat it as either String or String?, and checks nothing at compile time.

    // Option 1, the safe one: declare it nullable at the boundary and handle the null in Kotlin.
    val description: String? = item.description
    println(description ?: "(no description)") // -> (no description)

    // Option 2: declare it non-null. Kotlin inserts a check right here, so a null fails
    // at the boundary instead of three calls later.
    try {
        val strict: String = item.description
        println(strict)
    } catch (e: NullPointerException) {
        println(e.message) // -> getDescription(...) must not be null
    }

    // Option 3, the trap: let inference choose. 'inferred' keeps the platform type String!,
    // so nothing is checked here and the null travels on until something dereferences it.
    val inferred = item.description
    println("title: $inferred") // -> title: null
    try {
        println(inferred.length)
    } catch (e: NullPointerException) {
        println(e.message) // -> Cannot invoke "String.length()" because "inferred" is null
    }

    // With JSpecify annotations (@NullMarked package, @Nullable getter) there is no platform type:
    // Kotlin sees String? and the compiler forces you to deal with it.
    val v2 = SupplierItemV2("SUP-7781", null)
    // val title: String = v2.description  // error: initializer type mismatch: expected 'String', actual 'String?'.
    println(v2.description?.uppercase() ?: v2.code) // -> SUP-7781
    println(v2.code.length) // -> 8
}
