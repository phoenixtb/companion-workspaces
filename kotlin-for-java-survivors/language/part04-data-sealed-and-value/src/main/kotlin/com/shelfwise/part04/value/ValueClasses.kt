package com.shelfwise.part04.value

// A value class wraps exactly one value. At runtime it is (usually) just that value.
@JvmInline
value class Sku(val value: String) {
    init {
        require(value.matches(Regex("SHW-\\d{4}"))) { "bad SKU: $value" }
    }

    override fun toString() = value
}

@JvmInline
value class Barcode(val digits: String) {
    init {
        require(digits.length == 13 && digits.all { it.isDigit() }) { "bad EAN-13: $digits" }
    }
}

@JvmInline
value class Paise(val amount: Long) {
    operator fun plus(other: Paise): Paise = Paise(amount + other.amount) // enables '+' (Part 6)
    fun format(): String = "₹${amount / 100}.${(amount % 100).toString().padStart(2, '0')}"
}

fun shelfLabel(sku: Sku, barcode: Barcode, price: Paise): String =
    "$sku [${barcode.digits}] ${price.format()}"

fun basketTotal(prices: List<Paise>): Paise {
    var total = Paise(0)
    for (price in prices) total += price
    return total
}

fun main() {
    val sku = Sku("SHW-1001")
    val barcode = Barcode("8901234567890")
    println(shelfLabel(sku, barcode, Paise(16_500))) // -> SHW-1001 [8901234567890] ₹165.00
    // shelfLabel(barcode, sku, Paise(16_500))  // error: argument type mismatch: actual type is 'Barcode', but 'Sku' was expected.

    try {
        Sku("1001")
    } catch (e: IllegalArgumentException) {
        println(e.message) // -> bad SKU: 1001
    }

    println(basketTotal(listOf(Paise(16_500), Paise(3_050))).format()) // -> ₹195.50
    println(Sku("SHW-1001") == Sku("SHW-1001")) // -> true
    // Sku("SHW-1001") === Sku("SHW-1001")  // error: identity equality for arguments of types 'Sku' and 'Sku' is prohibited.
}
