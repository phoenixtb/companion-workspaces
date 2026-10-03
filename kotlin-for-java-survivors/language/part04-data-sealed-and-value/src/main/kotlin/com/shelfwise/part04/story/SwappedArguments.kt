package com.shelfwise.part04.story

// The bug from Pune: two Strings with different meanings, swapped at the call site. It compiles.
fun shelfLabel(sku: String, barcode: String, pricePaise: Long): String =
    "$sku [$barcode] ₹${pricePaise / 100}"

fun main() {
    val sku = "SHW-1001"
    val barcode = "8901234567890"
    println(shelfLabel(barcode, sku, 16_500)) // -> 8901234567890 [SHW-1001] ₹165
}
