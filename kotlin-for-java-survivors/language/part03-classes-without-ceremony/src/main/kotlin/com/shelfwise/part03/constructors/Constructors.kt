package com.shelfwise.part03.constructors

// The primary constructor is part of the class header. 'val'/'var' parameters become properties;
// plain parameters ('openedYear') are visible only to initialisers and init blocks.
class Store(val code: String, val city: String, openedYear: Int, val aisles: Int = 12) {

    // Initialisers and init blocks run top to bottom, interleaved, as one constructor body.
    val ageInYears: Int = 2026 - openedYear.also { println("1. property initialiser") }

    init {
        println("2. first init block")
        // println(label)  // error: variable 'label' must be initialized.
        require(code.matches(Regex("[A-Z]{3}-\\d{3}"))) { "bad store code: $code" }
    }

    val label: String = "$code ($city)".also { println("3. second property initialiser") }

    init {
        println("4. second init block")
    }

    // A secondary constructor must delegate to the primary one, which runs first.
    constructor(csvLine: String) : this(
        code = csvLine.substringBefore(','),
        city = csvLine.substringAfter(',').substringBefore(','),
        openedYear = csvLine.substringAfterLast(',').toInt(),
    ) {
        println("5. secondary constructor body")
    }
}

fun main() {
    val pune = Store("PUN-014", "Pune", openedYear = 2019)
    // -> 1. property initialiser
    // -> 2. first init block
    // -> 3. second property initialiser
    // -> 4. second init block
    println("${pune.label}, ${pune.ageInYears} years, ${pune.aisles} aisles") // -> PUN-014 (Pune), 7 years, 12 aisles

    val blr = Store("BLR-003,Bengaluru,2021")
    // -> 1. property initialiser
    // -> 2. first init block
    // -> 3. second property initialiser
    // -> 4. second init block
    // -> 5. secondary constructor body
    println(blr.label) // -> BLR-003 (Bengaluru)
}
