package com.shelfwise.part03.objects

// object: a class and its single instance in one declaration. Initialised lazily and
// thread-safely on first access, by the JVM's class initialisation, like a holder-idiom singleton.
object TaxTable {
    private val gstByCategory = mapOf("staples" to 5, "snacks" to 12, "household" to 18)

    fun gstPercent(category: String): Int = gstByCategory[category] ?: 18
}

interface IdSource {
    fun next(): Sku
}

class Sku private constructor(val value: String) {
    override fun toString() = value

    // A companion object holds what Java would make static: factories and constants.
    // Unlike statics, it is a real object, so it can implement an interface.
    companion object : IdSource {
        const val PREFIX = "SHW" // const in a companion becomes a static field on Sku
        private var issued = 0

        override fun next(): Sku = Sku("$PREFIX-${++issued}")

        @JvmStatic // also generate a static Sku.parse(...) for Java callers
        fun parse(raw: String): Sku {
            require(raw.startsWith("$PREFIX-")) { "not a Shelfwise SKU: $raw" }
            return Sku(raw)
        }
    }
}

fun issueTwo(source: IdSource): String = "${source.next()}, ${source.next()}"

interface PriceListener {
    fun onChange(sku: String, newPaise: Long)
}

fun main() {
    println(TaxTable.gstPercent("snacks")) // -> 12

    println(Sku.next()) // -> SHW-1
    println(issueTwo(Sku)) // -> SHW-2, SHW-3
    println(Sku.parse("SHW-77")) // -> SHW-77
    // Sku("SHW-9")  // error: cannot access 'constructor(value: String): Sku': it is private in 'com.shelfwise.part03.objects.Sku'.

    // Object expression: Java's anonymous class. Captured locals can be modified.
    var changes = 0
    val listener = object : PriceListener {
        override fun onChange(sku: String, newPaise: Long) {
            changes++ // a Java anonymous class could only read an effectively final local
            println("$sku -> $newPaise")
        }
    }
    listener.onChange("SHW-1", 15_900) // -> SHW-1 -> 15900
    listener.onChange("SHW-2", 8_900) // -> SHW-2 -> 8900
    println(changes) // -> 2
}
