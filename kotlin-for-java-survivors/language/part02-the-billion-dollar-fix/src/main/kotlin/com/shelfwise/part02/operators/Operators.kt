package com.shelfwise.part02.operators

class Promotion(val code: String, val percentOff: Int)

class Product(val name: String, val promotion: Promotion?, val brand: String?)

fun main() {
    val ghee = Product("Desi Ghee 1L", Promotion("DIWALI10", 10), brand = null)
    val rice = Product("Sona Masoori 5kg", promotion = null, brand = "Shelfwise Basics")

    // val code: String = ghee.promotion.code  // error: only safe (?.) or non-null asserted (!!.) calls are allowed on a nullable receiver of type 'Promotion?'.

    // ?. : call if not null, otherwise the whole expression is null.
    println(ghee.promotion?.code) // -> DIWALI10
    println(rice.promotion?.code) // -> null

    // ?: (Elvis): the fallback when the left side is null.
    println(rice.promotion?.percentOff ?: 0) // -> 0
    println(ghee.brand ?: "Unbranded") // -> Unbranded

    // ?.let { } : run a block only for non-null values; 'it' is smart-cast to the non-null type.
    rice.brand?.let { println("Brand page: /brands/${it.lowercase().replace(' ', '-')}") } // -> Brand page: /brands/shelfwise-basics
    ghee.brand?.let { println("never printed") }

    // as? : a cast that yields null instead of throwing ClassCastException.
    val scanned: Any = 8_901_234_567_890L
    val barcode: String? = scanned as? String
    println(barcode ?: "not a string barcode") // -> not a string barcode

    // !! : "I know better". Throws NullPointerException when you don't.
    try {
        println(rice.promotion!!.code)
    } catch (e: NullPointerException) {
        println("NPE, message: ${e.message}") // -> NPE, message: null
    }
}
