package com.shelfwise.part02.story

import com.supplier.sdk.SupplierItem

// The line from the staging crash: no '?', no '!!', and still a NullPointerException.
fun shelfTitle(item: SupplierItem): String = item.description.uppercase()

fun main() {
    val uncatalogued = SupplierItem("SUP-7781", null, listOf())
    try {
        println(shelfTitle(uncatalogued))
    } catch (e: NullPointerException) {
        println(e.message) // -> getDescription(...) must not be null
    }
}
