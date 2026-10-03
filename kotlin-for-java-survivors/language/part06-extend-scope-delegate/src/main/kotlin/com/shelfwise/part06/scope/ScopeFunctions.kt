package com.shelfwise.part06.scope

class ProductDraft {
    var sku = ""
    var name = ""
    var pricePaise = 0L
    val tags = mutableListOf<String>()
}

fun findName(sku: String): String? = mapOf("SHW-1001" to "Toor Dal 1kg")[sku]

class Reviewer(val name: String) {
    // Inside run, unqualified names resolve against the draft first: 'name' is the product's name.
    fun sign(draft: ProductDraft) = draft.run { "$name, reviewed by ${this@Reviewer.name}" }
}

fun main() {
    // apply: configure an object and return it. Inside, 'this' is the object.
    val draft = ProductDraft().apply {
        sku = "SHW-1001"
        name = "Toor Dal 1kg"
        pricePaise = 16_500
        tags += "staples"
    }

    // also: a side effect that returns the object unchanged. Inside, 'it' is the object.
    val same = draft.also { println("created ${it.sku}") } // -> created SHW-1001
    println(same === draft) // -> true

    // let: transform a value, most often after ?. to run only for non-null.
    println(findName("SHW-1001")?.let { "$it (${it.length} chars)" }) // -> Toor Dal 1kg (12 chars)
    println(findName("SHW-9999")?.let { "found $it" } ?: "no such SKU") // -> no such SKU

    // run: compute a result from an object, with 'this' as the receiver.
    println(draft.run { "$sku: $name at ${pricePaise / 100}" }) // -> SHW-1001: Toor Dal 1kg at 165

    // with: the same as run, but the object is an argument. Reads well for "with this object, do...".
    println(with(draft.tags) { joinToString(prefix = "#") }) // -> #staples

    // takeIf: the value if it passes the check, otherwise null.
    println(15.takeIf { it in 1..90 }) // -> 15
    println(500.takeIf { it in 1..90 }) // -> null

    println(Reviewer("Lena").sign(draft)) // -> Toor Dal 1kg, reviewed by Lena
}
