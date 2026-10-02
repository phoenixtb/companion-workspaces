package com.shelfwise.part02.importer

import com.supplier.sdk.SupplierItem

class ImportedProduct(val sku: String, val title: String, val allergens: List<String>) {
    override fun toString() = "$sku | $title | allergens=$allergens"
}

class ImportReport {
    val imported = mutableListOf<ImportedProduct>()
    val rejected = mutableListOf<String>()
}

// Step 1: validation returns null for "no valid SKU" instead of throwing.
fun normaliseSku(raw: String?): String? =
    raw?.trim()?.uppercase()?.takeIf { it.matches(Regex("SUP-\\d{4}")) }

// Step 2: every platform value gets an explicit Kotlin type at the boundary.
fun titleFor(item: SupplierItem, sku: String): String {
    val description: String? = item.description // String! stops here
    return description?.trim()?.takeIf { it.isNotEmpty() } ?: "Uncatalogued item $sku"
}

// A Java List<String> is (Mutable)List<String!>!: the list and each element may be null.
fun allergensOf(item: SupplierItem): List<String> {
    val raw: List<String?>? = item.allergens
    return raw.orEmpty().filterNotNull().map { it.lowercase() }
}

// Step 3: the import loop. After the null check, 'sku' is smart-cast to String.
fun importAll(items: List<SupplierItem>): ImportReport {
    val report = ImportReport()
    for (item in items) {
        val sku = normaliseSku(item.code)
        if (sku == null) {
            report.rejected += "rejected code '${item.code}'"
            continue
        }
        report.imported += ImportedProduct(sku, titleFor(item, sku), allergensOf(item))
    }
    return report
}

fun main() {
    val feed = listOf(
        SupplierItem(" sup-7781 ", "Durum Wheat Penne 500g", listOf("Gluten")),
        SupplierItem("SUP-7782", null, null), // not catalogued yet, allergens unknown
        SupplierItem("SUP-7783", "  ", listOf("Milk", null, "Soy")), // blank text, a null element
        SupplierItem(null, "Mystery Item", listOf()), // no code at all
    )

    val report = importAll(feed)
    for (product in report.imported) println(product)
    // -> SUP-7781 | Durum Wheat Penne 500g | allergens=[gluten]
    // -> SUP-7782 | Uncatalogued item SUP-7782 | allergens=[]
    // -> SUP-7783 | Uncatalogued item SUP-7783 | allergens=[milk, soy]
    println(report.rejected) // -> [rejected code 'null']
}
