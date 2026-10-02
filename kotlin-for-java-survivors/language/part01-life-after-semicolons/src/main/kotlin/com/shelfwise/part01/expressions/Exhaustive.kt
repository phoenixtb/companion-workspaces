package com.shelfwise.part01.expressions

enum class Band { OUT, LOW, OK } // enums are Part 3's topic; this one is just three constants

fun reorderAction(band: Band) {
    // A when *statement* over an enum must still cover every constant, unlike a Java switch statement:
    // when (band) { Band.OUT -> println("reorder now") }  // error: 'when' expression must be exhaustive. Add the 'LOW', 'OK' branches or an 'else' branch.
    when (band) {
        Band.OUT -> println("reorder now")
        Band.LOW -> println("reorder this week")
        Band.OK -> {} // deliberately nothing, and now that is visible in the code
    }
}

fun main() {
    reorderAction(Band.OUT) // -> reorder now
    reorderAction(Band.OK)
    reorderAction(Band.LOW) // -> reorder this week
}
