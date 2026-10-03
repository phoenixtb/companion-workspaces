package com.shelfwise.part09.importer

import com.shelfwise.part09.domain.Outcome
import java.io.IOException
import java.io.Reader
import java.io.StringReader

// Step 1: what a supplier line can become. Expected problems are data, not exceptions.
data class PriceUpdate(val sku: String, val newPaise: Long)

sealed interface Rejection {
    val line: Int

    data class Malformed(override val line: Int, val text: String) : Rejection
    data class UnknownSku(override val line: Int, val sku: String) : Rejection
    data class PriceJump(override val line: Int, val sku: String, val fromPaise: Long, val toPaise: Long) : Rejection
}

// Step 2: validation is a pure function that returns an Outcome (from domain/DomainErrors.kt).
fun validate(line: Int, text: String, current: Map<String, Long>): Outcome<PriceUpdate, Rejection> {
    val parts = text.split(",")
    val paise = parts.getOrNull(1)?.trim()?.toLongOrNull()
    if (parts.size != 2 || paise == null) return Outcome.Err(Rejection.Malformed(line, text))
    val sku = parts[0].trim()
    val old = current[sku] ?: return Outcome.Err(Rejection.UnknownSku(line, sku))
    if (paise > old * 3 / 2) return Outcome.Err(Rejection.PriceJump(line, sku, old, paise))
    return Outcome.Ok(PriceUpdate(sku, paise))
}

data class ImportReport(val accepted: List<PriceUpdate>, val rejected: List<Rejection>)

// Step 3: reading stays exception-based: an I/O failure is not a property of one line.
// @Throws tells Java callers (and javac) about it.
@Throws(IOException::class)
fun importPrices(source: Reader, current: Map<String, Long>): ImportReport {
    val accepted = mutableListOf<PriceUpdate>()
    val rejected = mutableListOf<Rejection>()
    source.buffered().useLines { lines ->
        lines.forEachIndexed { index, text ->
            when (val outcome = validate(index + 1, text, current)) {
                is Outcome.Ok -> accepted += outcome.value
                is Outcome.Err -> rejected += outcome.error
            }
        }
    }
    return ImportReport(accepted, rejected)
}

// Step 4: a boundary that turns I/O failures into a Result, and lets everything else through:
// cancellation, Errors and bugs keep propagating, unlike with runCatching.
inline fun <T> catchingIo(block: () -> T): Result<T> =
    try {
        Result.success(block())
    } catch (e: IOException) {
        Result.failure(e)
    }

fun describe(rejection: Rejection): String = when (rejection) {
    is Rejection.Malformed -> "line ${rejection.line}: can't read '${rejection.text}'"
    is Rejection.UnknownSku -> "line ${rejection.line}: unknown SKU ${rejection.sku}"
    is Rejection.PriceJump -> "line ${rejection.line}: ${rejection.sku} jumps ${rejection.fromPaise} -> ${rejection.toPaise}, needs review"
}

// Step 5: run it on a good file and a broken connection.
fun main() {
    val current = mapOf("SHW-1001" to 16_500L, "SHW-2040" to 72_000L)
    val supplierFile = "SHW-1001,15900\nSHW-2040,180000\nSHW-7777,500\nnot a price line"

    val report = importPrices(StringReader(supplierFile), current) // a StringReader can't fail
    println(report.accepted) // -> [PriceUpdate(sku=SHW-1001, newPaise=15900)]
    report.rejected.forEach { println(describe(it)) }
    // -> line 2: SHW-2040 jumps 72000 -> 180000, needs review
    // -> line 3: unknown SKU SHW-7777
    // -> line 4: can't read 'not a price line'

    val brokenConnection = object : Reader() {
        override fun read(buffer: CharArray, offset: Int, length: Int): Int = throw IOException("connection reset")

        override fun close() {}
    }
    val failed = catchingIo { importPrices(brokenConnection, current) }
    println("import failed: ${failed.exceptionOrNull()?.message}") // -> import failed: connection reset
}
