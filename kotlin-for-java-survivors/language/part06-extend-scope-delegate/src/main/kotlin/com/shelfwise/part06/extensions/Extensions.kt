package com.shelfwise.part06.extensions

// An extension function: called like a member, compiled to a static method that takes the receiver first.
fun String.isSku(): Boolean = matches(Regex("SHW-\\d{4}"))

// Extension properties have no backing field: they are getters (and setters) only.
val Int.rupees: Long get() = this * 100L

fun Long.formatPaise(): String = "₹${this / 100}.${(this % 100).toString().padStart(2, '0')}"

// A nullable receiver: callable on null, and 'this' may be null inside.
fun String?.orUnknown(): String = this ?: "UNKNOWN"

open class Shelf
class ChilledShelf : Shelf()

fun Shelf.kind() = "shelf"
fun ChilledShelf.kind() = "chilled shelf"

class Label(val text: String) {
    fun render() = "member: $text"
}

// A member always wins over an extension with the same signature. The compiler warns.
fun Label.render() = "extension: $text"

fun main() {
    println("SHW-1001".isSku()) // -> true
    println(165.rupees) // -> 16500
    println(16_550L.formatPaise()) // -> ₹165.50

    val missing: String? = null
    println(missing.orUnknown()) // -> UNKNOWN

    // Extensions dispatch on the static type: there is no virtual call to override.
    val shelf: Shelf = ChilledShelf()
    println(shelf.kind()) // -> shelf
    println(ChilledShelf().kind()) // -> chilled shelf

    println(Label("Toor Dal").render()) // -> member: Toor Dal
}
