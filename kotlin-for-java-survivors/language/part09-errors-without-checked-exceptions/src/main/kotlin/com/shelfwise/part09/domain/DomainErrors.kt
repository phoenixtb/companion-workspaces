package com.shelfwise.part09.domain

// A generic outcome type. Nothing (Part 2) and 'out' (Part 8) make Ok and Err fit any Outcome<T, E>.
sealed interface Outcome<out T, out E> {
    data class Ok<out T>(val value: T) : Outcome<T, Nothing>
    data class Err<out E>(val error: E) : Outcome<Nothing, E>
}

// The errors a price lookup can produce, as a closed set the caller must handle.
sealed interface LookupError {
    data class UnknownSku(val sku: String) : LookupError
    data class Suspended(val sku: String, val reason: String) : LookupError
}

fun lookupPrice(sku: String): Outcome<Long, LookupError> = when (sku) {
    "SHW-1001" -> Outcome.Ok(16_500)
    "SHW-2040" -> Outcome.Err(LookupError.Suspended(sku, "supplier recall"))
    else -> Outcome.Err(LookupError.UnknownSku(sku))
}

fun shelfLabel(sku: String): String = when (val outcome = lookupPrice(sku)) {
    is Outcome.Ok -> "$sku ₹${outcome.value / 100}"
    is Outcome.Err -> when (val error = outcome.error) {
        is LookupError.UnknownSku -> "$sku NOT FOUND"
        is LookupError.Suspended -> "$sku UNAVAILABLE (${error.reason})"
    }
}

fun main() {
    listOf("SHW-1001", "SHW-2040", "SHW-0000").forEach { println(shelfLabel(it)) }
    // -> SHW-1001 ₹165
    // -> SHW-2040 UNAVAILABLE (supplier recall)
    // -> SHW-0000 NOT FOUND
}
