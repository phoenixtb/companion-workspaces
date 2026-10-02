package com.shelfwise.part01.strings

fun main() {
    val name = "Basmati Rice 5kg"
    val pricePaise = 89_900L

    // $name for a simple name, ${...} for any expression.
    println("$name costs ₹${pricePaise / 100}") // -> Basmati Rice 5kg costs ₹899

    // Raw strings: no escaping, real newlines. trimMargin() strips everything up to '|'.
    val label = """
        |SHELFWISE
        |$name
        |₹${pricePaise / 100}
    """.trimMargin()
    println(label)
    // -> SHELFWISE
    // -> Basmati Rice 5kg
    // -> ₹899

    // Raw strings have no escapes, so a literal "$schema" is read as a template:
    // val broken = """{"$schema": "..."}"""  // error: unresolved reference 'schema'.
}
