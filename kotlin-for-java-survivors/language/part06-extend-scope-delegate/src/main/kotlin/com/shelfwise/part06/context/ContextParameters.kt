package com.shelfwise.part06.context

class AuditLog(private val component: String) {
    val lines = mutableListOf<String>()

    fun info(message: String) {
        lines += "[$component] $message"
    }
}

data class Region(val code: String, val vatPercent: Int)

// Context parameters (Stable since 2.4.0): declared once, supplied by the caller's context.
context(region: Region, log: AuditLog)
fun priceWithVat(pricePaise: Long): Long {
    val total = pricePaise * (100 + region.vatPercent) / 100
    log.info("${region.code}: $pricePaise -> $total")
    return total
}

// A caller with the same context passes it on without naming it.
context(region: Region, log: AuditLog)
fun basketTotal(prices: List<Long>): Long = prices.sumOf { priceWithVat(it) }

class Transaction(val id: String) {
    private val writes = mutableListOf<String>()

    fun write(row: String) {
        writes += row
    }

    fun commit(): List<String> = writes.toList()
}

context(tx: Transaction)
fun savePrice(sku: String, pricePaise: Long) = tx.write("$sku=$pricePaise")

// '_': this function never touches the Transaction; it only passes it on, implicitly, to savePrice.
context(_: Transaction)
fun reprice(skus: List<String>, pricePaise: Long) = skus.forEach { savePrice(it, pricePaise) }

// The block's type has a context: inside it, a Transaction is available implicitly.
fun transaction(id: String, block: context(Transaction) () -> Unit) {
    val tx = Transaction(id)
    context(tx) { block() }
    println("commit ${tx.id}: ${tx.commit()}")
}

fun main() {
    val log = AuditLog("pricing")
    // context(a, b) { } (standard library) puts values into the context for the block.
    context(Region("MH", 5), log) {
        println(basketTotal(listOf(10_000, 20_000))) // -> 31500
    }
    log.lines.forEach(::println)
    // -> [pricing] MH: 10000 -> 10500
    // -> [pricing] MH: 20000 -> 21000

    // priceWithVat(10_000)  // error: no context argument for 'region: Region' found.

    transaction("tx-1") {
        savePrice("SHW-1001", 15_900)
        reprice(listOf("SHW-2040", "SHW-2041"), 69_000)
    } // -> commit tx-1: [SHW-1001=15900, SHW-2040=69000, SHW-2041=69000]
}
