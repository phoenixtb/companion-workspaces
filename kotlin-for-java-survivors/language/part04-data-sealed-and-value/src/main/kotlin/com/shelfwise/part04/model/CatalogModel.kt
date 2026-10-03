package com.shelfwise.part04.model

// Step 1: identifiers and money as value classes. Each validates once, at construction.
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

    override fun toString() = digits
}

@JvmInline
value class Paise(val amount: Long) {
    init {
        require(amount >= 0) { "negative price: $amount" }
    }

    override fun toString() = "₹${amount / 100}.${(amount % 100).toString().padStart(2, '0')}"
}

// Step 2: stock as a closed set of states instead of an int plus flags.
sealed interface StockLevel {
    data class InStock(val units: Int) : StockLevel
    data class Low(val units: Int) : StockLevel
    data object OutOfStock : StockLevel
}

// Step 2 (continued): the product itself is an immutable data class. Changes produce new instances.
data class Product(
    val sku: Sku,
    val barcode: Barcode,
    val name: String,
    val price: Paise,
    val stock: StockLevel,
    val active: Boolean = true,
)

// Step 3: everything that can happen to a product, as a sealed hierarchy of events.
sealed interface CatalogEvent {
    val sku: Sku

    data class ProductAdded(val product: Product) : CatalogEvent {
        override val sku: Sku get() = product.sku
    }

    data class PriceChanged(override val sku: Sku, val from: Paise, val to: Paise) : CatalogEvent
    data class StockChanged(override val sku: Sku, val level: StockLevel) : CatalogEvent
    data class Discontinued(override val sku: Sku, val reason: String) : CatalogEvent
}

// Step 4: applying an event is a when over the hierarchy, with copy() doing the update.
fun applyEvent(current: Product?, event: CatalogEvent): Product = when (event) {
    is CatalogEvent.ProductAdded -> event.product
    is CatalogEvent.PriceChanged -> requireNotNull(current).copy(price = event.to)
    is CatalogEvent.StockChanged -> requireNotNull(current).copy(stock = event.level)
    is CatalogEvent.Discontinued -> requireNotNull(current).copy(active = false, stock = StockLevel.OutOfStock)
}

fun describe(event: CatalogEvent): String = when (event) {
    is CatalogEvent.ProductAdded -> "added ${event.product.name}"
    is CatalogEvent.PriceChanged -> "price ${event.from} -> ${event.to}"
    is CatalogEvent.StockChanged -> "stock -> ${event.level}"
    is CatalogEvent.Discontinued -> "discontinued (${event.reason})"
}

// The label function from the incident, now with types that can't be swapped.
fun shelfLabel(sku: Sku, barcode: Barcode, price: Paise): String = "$sku [$barcode] $price"

fun main() {
    val sku = Sku("SHW-1001")
    val events = listOf(
        CatalogEvent.ProductAdded(
            Product(sku, Barcode("8901234567890"), "Toor Dal 1kg", Paise(16_500), StockLevel.InStock(120)),
        ),
        CatalogEvent.PriceChanged(sku, from = Paise(16_500), to = Paise(15_900)),
        CatalogEvent.StockChanged(sku, StockLevel.Low(6)),
        CatalogEvent.Discontinued(sku, reason = "supplier change"),
    )

    var product: Product? = null
    for (event in events) {
        product = applyEvent(product, event)
        println("${event.sku}: ${describe(event)}")
    }
    // -> SHW-1001: added Toor Dal 1kg
    // -> SHW-1001: price ₹165.00 -> ₹159.00
    // -> SHW-1001: stock -> Low(units=6)
    // -> SHW-1001: discontinued (supplier change)

    println(product)
    // -> Product(sku=SHW-1001, barcode=8901234567890, name=Toor Dal 1kg, price=₹159.00, stock=OutOfStock, active=false)

    val final = requireNotNull(product)
    println(shelfLabel(final.sku, final.barcode, final.price)) // -> SHW-1001 [8901234567890] ₹159.00
    // shelfLabel(final.barcode, final.sku, final.price)  // error: argument type mismatch: actual type is 'Barcode', but 'Sku' was expected.
}
