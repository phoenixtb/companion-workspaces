package com.shelfwise.part02.platform

import java.util.Optional
import kotlin.jvm.optionals.getOrNull

// A Java API that returns Optional, as many repositories and clients do.
fun findPromotionCode(sku: String): Optional<String> =
    if (sku == "SUP-7781") Optional.of("DIWALI10") else Optional.empty()

fun main() {
    // getOrNull() turns Optional<T> into T?, and from there the null operators take over.
    val code: String? = findPromotionCode("SUP-7782").getOrNull()
    println(code ?: "no promotion") // -> no promotion
    println(findPromotionCode("SUP-7781").getOrNull()?.lowercase()) // -> diwali10
}
