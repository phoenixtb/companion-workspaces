package com.shelfwise.part02.platform

// A non-null parameter is a promise Java callers can break. Kotlin checks it on entry.
fun normalise(sku: String): String = sku.trim().uppercase()
