package com.shelfwise.part05.rules

data class Line(val sku: String, val category: String, val pricePaise: Long, val quantity: Int)

// Step 1: a rule is a fun interface, so a lambda, a function reference or an object can be one.
fun interface PriceRule {
    fun price(line: Line): Long // the new unit price
}

// Step 2: rule factories. Each returns a lambda that captures its configuration.
fun percentOff(percent: Int, appliesTo: (Line) -> Boolean = { true }): PriceRule =
    PriceRule { line -> if (appliesTo(line)) line.pricePaise * (100 - percent) / 100 else line.pricePaise }

fun buyMorePayLess(minQuantity: Int, percent: Int): PriceRule =
    percentOff(percent) { it.quantity >= minQuantity }

// Step 3: an ordinary function that will become a rule through a reference.
fun roundToRupee(line: Line): Long = line.pricePaise / 100 * 100

// Step 4: composition. A higher-order function that takes rules and returns a rule.
fun chain(rules: List<PriceRule>): PriceRule = PriceRule { line ->
    var price = line.pricePaise
    for (rule in rules) price = rule.price(line.copy(pricePaise = price))
    price
}

// Step 5: an inline helper that reports each amount to an audit callback; the block never becomes an object.
inline fun auditing(audit: (String) -> Unit, sku: String, block: () -> Long): Long {
    val amount = block()
    audit("$sku -> $amount")
    return amount
}

fun priceBasket(lines: List<Line>, rule: PriceRule, audit: (String) -> Unit): Long {
    var total = 0L
    for (line in lines) {
        if (line.quantity == 0) continue // nothing to price, nothing to audit
        total += auditing(audit, line.sku) { rule.price(line) * line.quantity }
    }
    return total
}

fun main() {
    val festiveGrocery = chain(
        listOf(
            percentOff(10) { it.category == "staples" },
            buyMorePayLess(minQuantity = 3, percent = 5),
            PriceRule(::roundToRupee),
        ),
    )
    val basket = listOf(
        Line("SHW-1001", "staples", 16_550, 3),
        Line("SHW-1002", "snacks", 3_050, 1),
        Line("SHW-1003", "staples", 9_990, 0),
    )

    val log = mutableListOf<String>()
    println(priceBasket(basket, festiveGrocery, log::add)) // -> 45300
    log.forEach(::println)
    // -> SHW-1001 -> 42300
    // -> SHW-1002 -> 3000
}
