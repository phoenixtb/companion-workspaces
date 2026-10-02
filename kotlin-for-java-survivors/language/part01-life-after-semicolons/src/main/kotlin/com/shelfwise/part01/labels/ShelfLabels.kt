package com.shelfwise.part01.labels

// Part 3 explains this line in full: a class whose constructor declares read-only properties.
class Product(val name: String, val pricePaise: Long, val unitsOnShelf: Int, val category: String)

const val STORE_CODE = "PUN-014"

// Step 1: one formatting function with a default instead of overloads.
fun formatPrice(paise: Long, currency: String = "₹"): String =
    "$currency${paise / 100}.${(paise % 100).toString().padStart(2, '0')}"

// Step 2: the badge rules as a single when expression. First matching branch wins.
fun badgeFor(product: Product): String = when {
    product.unitsOnShelf == 0 -> "SOLD OUT"
    product.unitsOnShelf < 10 -> "LAST FEW"
    product.category == "Bakery" -> "FRESH TODAY"
    else -> ""
}

// Step 3: one label line, padded so prices line up on the shelf edge.
fun renderLabel(product: Product, nameWidth: Int = 20): String {
    val name = product.name.padEnd(nameWidth)
    val price = formatPrice(product.pricePaise).padStart(9)
    return "$name$price  ${badgeFor(product)}".trimEnd()
}

// Step 4: fill shelves slot by slot; a labelled break stops both loops when products run out.
fun planAisle(products: List<Product>, shelves: Int, slotsPerShelf: Int): List<String> {
    val plan = mutableListOf<String>()
    var next = 0
    shelves@ for (shelf in 1..shelves) {
        for (slot in 1..slotsPerShelf) {
            if (next == products.size) break@shelves
            plan += "S$shelf/$slot ${renderLabel(products[next])}"
            next++
        }
    }
    val unplaced = products.size - next
    if (unplaced > 0) plan += "No slot for $unplaced product(s)"
    return plan
}

// Step 5: the label printer speaks JSON with a literal "$type" key, so use a $$ raw string.
fun printerPayload(product: Product): String =
    $$"""{"$type": "shelf-label", "store": "$$STORE_CODE", "name": "$${product.name}", "price": "$${formatPrice(product.pricePaise)}"}"""

fun main() {
    val aisleSeven = listOf(
        Product("Salted Butter 500g", 28_500, 42, "Dairy"),
        Product("Sourdough Loaf", 18_000, 12, "Bakery"),
        Product("Paneer 200g", 9_500, 6, "Dairy"),
        Product("Greek Yoghurt 400g", 7_000, 0, "Dairy"),
        Product("Masala Oats 1kg", 32_000, 20, "Breakfast"),
    )

    for (line in planAisle(aisleSeven, shelves = 2, slotsPerShelf = 2)) {
        println(line)
    }
    // -> S1/1 Salted Butter 500g    ₹285.00
    // -> S1/2 Sourdough Loaf        ₹180.00  FRESH TODAY
    // -> S2/1 Paneer 200g            ₹95.00  LAST FEW
    // -> S2/2 Greek Yoghurt 400g     ₹70.00  SOLD OUT
    // -> No slot for 1 product(s)

    println(printerPayload(aisleSeven[1]))
    // -> {"$type": "shelf-label", "store": "PUN-014", "name": "Sourdough Loaf", "price": "₹180.00"}
}
