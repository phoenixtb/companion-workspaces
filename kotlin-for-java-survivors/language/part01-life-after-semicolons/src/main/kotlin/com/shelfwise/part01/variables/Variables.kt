package com.shelfwise.part01.variables

fun main() {
    val storeCount = 412 // inferred as Int; val = a final local
    var openStores = 398 // var = reassignable
    openStores += 2
    // storeCount = 413  // error: 'val' cannot be reassigned.

    val aisles = mutableListOf("Dairy", "Bakery")
    aisles += "Frozen" // the reference is final, the list it points to is not

    println("$openStores of $storeCount stores open") // -> 400 of 412 stores open
    println(aisles) // -> [Dairy, Bakery, Frozen]
}
