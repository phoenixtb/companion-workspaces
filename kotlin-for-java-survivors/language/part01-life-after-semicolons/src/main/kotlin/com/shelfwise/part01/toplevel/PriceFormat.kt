package com.shelfwise.part01.toplevel

// No class, no static: a function can live directly in a package.
// The compiler puts it in a class named after the file: PriceFormatKt.
fun formatPrice(paise: Long): String =
    if (paise < 0) "-" else "₹${paise / 100}.${(paise % 100).toString().padStart(2, '0')}"

fun main() {
    println(formatPrice(19_900)) // -> ₹199.00
    println(formatPrice(4_550)) // -> ₹45.50
    println(formatPrice(-1)) // -> -
}
