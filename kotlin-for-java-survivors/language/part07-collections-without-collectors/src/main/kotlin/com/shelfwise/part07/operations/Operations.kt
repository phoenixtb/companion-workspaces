package com.shelfwise.part07.operations

data class Product(val sku: String, val name: String, val category: String, val pricePaise: Long, val units: Int)

val catalog = listOf(
    Product("SHW-1001", "Toor Dal 1kg", "staples", 16_500, 120),
    Product("SHW-1002", "Moong Dal 1kg", "staples", 14_200, 0),
    Product("SHW-2040", "Basmati 5kg", "staples", 72_000, 35),
    Product("SHW-3001", "Ghee 500ml", "dairy", 34_000, 8),
    Product("SHW-3002", "Paneer 200g", "dairy", 9_000, 40),
    Product("SHW-5001", "Masala Chips", "snacks", 2_000, 300),
)

fun main() {
    // stream().map(...).collect(toList()) -> map
    println(catalog.filter { it.units == 0 }.map { it.sku }) // -> [SHW-1002]

    // Collectors.groupingBy -> groupBy (a LinkedHashMap: insertion order kept)
    val byCategory: Map<String, List<Product>> = catalog.groupBy { it.category }
    println(byCategory.mapValues { (_, products) -> products.size }) // -> {staples=3, dairy=2, snacks=1}

    // Collectors.toMap(Product::getSku, identity()) -> associateBy (duplicate keys: last wins, no exception)
    val bySku: Map<String, Product> = catalog.associateBy { it.sku }
    println(bySku.getValue("SHW-3001").name) // -> Ghee 500ml
    val relabelled = catalog + Product("SHW-3001", "Ghee 1L", "dairy", 64_000, 2)
    println(relabelled.associateBy { it.sku }.getValue("SHW-3001").name) // -> Ghee 1L

    // Collectors.partitioningBy -> partition, returning a Pair you destructure
    val (inStock, soldOut) = catalog.partition { it.units > 0 }
    println("${inStock.size} in stock, ${soldOut.size} sold out") // -> 5 in stock, 1 sold out

    // mapToLong(...).sum() -> sumOf; reduce with identity -> fold
    println(catalog.sumOf { it.pricePaise * it.units }) // -> 5732000
    println(catalog.fold(0) { total, product -> total + product.units }) // -> 503

    // max(comparing(...)) -> maxBy / maxByOrNull (no Optional)
    println(catalog.maxBy { it.pricePaise }.name) // -> Basmati 5kg
    println(catalog.filter { it.category == "frozen" }.maxByOrNull { it.pricePaise }) // -> null

    // flatMap, zip, chunked, windowed
    val tags = mapOf("SHW-1001" to listOf("protein", "staple"), "SHW-3002" to listOf("protein", "fresh"))
    println(tags.values.flatMap { it }.distinct()) // -> [protein, staple, fresh]
    println(listOf("A1", "A2", "A3").zip(listOf(120, 35))) // -> [(A1, 120), (A2, 35)]
    println(catalog.map { it.sku }.chunked(4)) // -> [[SHW-1001, SHW-1002, SHW-2040, SHW-3001], [SHW-3002, SHW-5001]]
    val dailySales = listOf(40, 42, 39, 61, 75)
    println(dailySales.windowed(3) { it.average().toInt() }) // -> [40, 47, 58]

    // Counting per key without building the groups: groupingBy + eachCount
    println(catalog.groupingBy { it.category }.eachCount()) // -> {staples=3, dairy=2, snacks=1}

    // isSorted / isSortedBy (Stable since 2.4.0)
    println(dailySales.isSorted()) // -> false
    println(catalog.isSortedBy { it.sku }) // -> true
}
