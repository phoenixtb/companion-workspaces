package com.shelfwise.part06.story

class AuditLog {
    val lines = mutableListOf<String>()
}

// Kabir's Friday version: region and log threaded through every layer by hand.
fun percentOff(pricePaise: Long, percent: Int, region: String, log: AuditLog): Long {
    val result = pricePaise * (100 - percent) / 100
    log.lines += "$region: -$percent% -> $result"
    return result
}

fun priceLine(pricePaise: Long, quantity: Int, region: String, log: AuditLog): Long =
    percentOff(pricePaise, 10, region, log) * quantity

fun priceBasket(lines: List<Pair<Long, Int>>, region: String, log: AuditLog): Long =
    lines.sumOf { (price, quantity) -> priceLine(price, quantity, region, log) }

fun main() {
    val log = AuditLog()
    println(priceBasket(listOf(16_500L to 2, 3_050L to 1), "MH", log)) // -> 32445
    println(log.lines) // -> [MH: -10% -> 14850, MH: -10% -> 2745]
}
