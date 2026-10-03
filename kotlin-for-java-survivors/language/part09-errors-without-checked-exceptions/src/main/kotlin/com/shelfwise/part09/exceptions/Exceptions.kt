package com.shelfwise.part09.exceptions

import java.io.StringReader

// try is an expression: the value of the try block or of the catch block.
fun parsePaise(text: String): Long? =
    try {
        text.trim().toLong()
    } catch (e: NumberFormatException) {
        null
    }

class PriceList(private val prices: MutableMap<String, Long>) {
    var frozen = false

    fun priceOf(sku: String): Long {
        require(sku.startsWith("SHW-")) { "not a Shelfwise SKU: $sku" } // caller's fault: IllegalArgumentException
        return prices[sku] ?: error("no price for $sku") // error() throws IllegalStateException, type Nothing
    }

    fun update(sku: String, paise: Long) {
        check(!frozen) { "price list is frozen" } // object's state is wrong: IllegalStateException
        prices[sku] = paise
    }
}

// Runs a block and prints the exception it throws, if any.
fun attempt(block: () -> Unit) {
    try {
        block()
    } catch (e: RuntimeException) {
        println("${e::class.simpleName}: ${e.message}")
    }
}

fun main() {
    println(parsePaise(" 16500 ")) // -> 16500
    println(parsePaise("16,500")) // -> null

    val list = PriceList(mutableMapOf("SHW-1001" to 16_500))
    attempt { list.priceOf("1001") } // -> IllegalArgumentException: not a Shelfwise SKU: 1001
    attempt { list.priceOf("SHW-2040") } // -> IllegalStateException: no price for SHW-2040
    attempt { list.frozen = true; list.update("SHW-1001", 1) } // -> IllegalStateException: price list is frozen

    // requireNotNull throws IllegalArgumentException, not the NullPointerException of Objects.requireNonNull.
    val supplierCode: String? = null
    attempt { requireNotNull(supplierCode) { "supplier code missing" } } // -> IllegalArgumentException: supplier code missing

    // use: try-with-resources as an extension on Closeable/AutoCloseable.
    val lines = StringReader("SHW-1001,16500\nSHW-2040,72000").buffered().use { it.readLines() }
    println(lines.size) // -> 2
}
