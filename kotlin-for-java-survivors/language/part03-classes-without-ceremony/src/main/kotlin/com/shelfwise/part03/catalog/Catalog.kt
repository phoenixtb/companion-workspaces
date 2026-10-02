package com.shelfwise.part03.catalog

// Step 1: categories carry their GST rate; parsing lives on the enum's companion.
enum class Category(val gstPercent: Int) {
    STAPLES(5),
    DAIRY(5),
    SNACKS(12),
    HOUSEHOLD(18);

    companion object {
        fun parse(raw: String): Category =
            entries.firstOrNull { it.name.equals(raw.trim(), ignoreCase = true) }
                ?: error("unknown category: '$raw'")
    }
}

// Step 2: the constructor is private, so every Product goes through a validating factory.
class Product private constructor(val sku: String, name: String, val category: Category, pricePaise: Long) {

    // Step 3: a validating setter. 'field' is the backing field.
    var name: String = name.trim()
        set(value) {
            require(value.isNotBlank()) { "name must not be blank" }
            field = value.trim()
        }

    // Readable everywhere, changed only through reprice().
    var pricePaise: Long = pricePaise
        private set

    // Explicit backing field: List to the world, MutableList inside the class.
    val priceHistory: List<Long>
        field = mutableListOf(pricePaise)

    // Computed on every read: no field at all.
    val shelfPricePaise: Long
        get() = pricePaise + pricePaise * category.gstPercent / 100

    init {
        require(sku.matches(SKU_FORMAT)) { "bad SKU: $sku" }
        require(pricePaise > 0) { "price must be positive, was $pricePaise" }
    }

    fun reprice(newPaise: Long) {
        require(newPaise > 0) { "price must be positive, was $newPaise" }
        pricePaise = newPaise
        priceHistory += newPaise
    }

    override fun toString() = "$sku $name [${category.name.lowercase()}] ₹${shelfPricePaise / 100}"

    // Step 4: the factory and the format rule live in the companion, like Java statics.
    companion object {
        private val SKU_FORMAT = Regex("SHW-\\d{4}")

        fun fromCsv(line: String): Product {
            val parts = line.split(',').map { it.trim() }
            require(parts.size == 4) { "expected sku,name,category,price but got: $line" }
            return Product(parts[0], parts[1], Category.parse(parts[2]), parts[3].toLong())
        }
    }
}

fun main() {
    val feed = listOf(
        "SHW-1001, Toor Dal 1kg, staples, 16500",
        "SHW-1002, Masala Chips 150g, SNACKS, 3000",
        "SHW-1003, Floor Cleaner 1L, household, 22000",
    )
    val products = feed.map { Product.fromCsv(it) }
    for (product in products) println(product)
    // -> SHW-1001 Toor Dal 1kg [staples] ₹173
    // -> SHW-1002 Masala Chips 150g [snacks] ₹33
    // -> SHW-1003 Floor Cleaner 1L [household] ₹259

    val dal = products.first()
    dal.reprice(15_900)
    dal.name = "  Toor Dal (Arhar) 1kg "
    println(dal) // -> SHW-1001 Toor Dal (Arhar) 1kg [staples] ₹166
    println(dal.priceHistory) // -> [16500, 15900]
    // dal.pricePaise = 1  // error: cannot access 'pricePaise': it is private in 'com.shelfwise.part03.catalog.Product'.

    try {
        Product.fromCsv("SHW-12, Paneer 200g, dairy, 9500")
    } catch (e: IllegalArgumentException) {
        println(e.message) // -> bad SKU: SHW-12
    }
    try {
        Product.fromCsv("SHW-1004, Cricket Bat, toys, 99900")
    } catch (e: IllegalStateException) {
        println(e.message) // -> unknown category: 'toys'
    }
}
