package com.shelfwise.part06.delegation

interface Catalog {
    fun priceOf(sku: String): Long?
    fun skus(): Set<String>

    // A default method that calls another interface method.
    fun pricedCount(): Int = skus().count { priceOf(it) != null }
}

class InMemoryCatalog(private val prices: Map<String, Long?>) : Catalog {
    override fun priceOf(sku: String) = prices[sku]
    override fun skus() = prices.keys
}

// 'by inner': the compiler writes every Catalog method as a call to 'inner', except those we override.
class CountingCatalog(private val inner: Catalog) : Catalog by inner {
    var lookups = 0
        private set

    override fun priceOf(sku: String): Long? {
        lookups++
        return inner.priceOf(sku)
    }
}

// 'var inner' looks like a swappable decorator, but 'by' captured the value at construction.
class SwitchableCatalog(var inner: Catalog) : Catalog by inner

fun main() {
    val catalog = CountingCatalog(InMemoryCatalog(mapOf("SHW-1001" to 16_500, "SHW-2040" to null)))

    println(catalog.priceOf("SHW-1001")) // -> 16500
    println(catalog.skus()) // -> [SHW-1001, SHW-2040]
    println(catalog.lookups) // -> 1

    // pricedCount() is forwarded to 'inner', and inner's calls to priceOf() never come back here.
    println(catalog.pricedCount()) // -> 1
    println(catalog.lookups) // -> 1

    val switchable = SwitchableCatalog(InMemoryCatalog(mapOf("SHW-1001" to 16_500)))
    switchable.inner = InMemoryCatalog(mapOf("SHW-1001" to 14_900))
    println(switchable.priceOf("SHW-1001")) // -> 16500
}
