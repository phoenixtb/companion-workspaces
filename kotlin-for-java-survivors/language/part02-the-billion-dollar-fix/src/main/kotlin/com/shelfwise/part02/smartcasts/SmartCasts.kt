package com.shelfwise.part02.smartcasts

open class Shelf(open val label: String?)

class Bay(var note: String?)

fun labelLength(shelf: Shelf): Int {
    // if (shelf.label != null) return shelf.label.length
    //   error: smart cast to 'String' is impossible, because 'label' is a property that has an open or custom getter.
    // A subclass could override 'label' with a getter that returns null on the second call.

    // Fix: read it once into a local val. Locals can't change behind your back.
    val label = shelf.label
    return if (label != null) label.length else 0
}

fun noteLength(bay: Bay): Int {
    // if (bay.note != null) return bay.note.length
    //   error: smart cast to 'String' is impossible, because 'note' is a mutable property that could be mutated concurrently.

    // Fix: let the safe-call chain carry the null, or copy to a local as above.
    return bay.note?.length ?: 0
}

fun describe(value: Any?): String {
    // Smart casts flow through conditions: after the null check, 'value' is Any; after 'is', String.
    if (value == null) return "nothing"
    if (value !is String) return "a ${value::class.simpleName}"
    return "text '${value.trim()}'" // smart cast: value is String here
}

fun main() {
    println(labelLength(Shelf("Spices"))) // -> 6
    println(noteLength(Bay(null))) // -> 0
    println(describe(null)) // -> nothing
    println(describe(42)) // -> a Int
    println(describe("  basmati ")) // -> text 'basmati'
}
