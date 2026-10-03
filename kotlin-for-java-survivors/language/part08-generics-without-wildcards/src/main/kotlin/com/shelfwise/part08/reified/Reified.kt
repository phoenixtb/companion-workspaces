package com.shelfwise.part08.reified

sealed interface CatalogEvent {
    val sku: String
}

data class PriceChanged(override val sku: String, val toPaise: Long) : CatalogEvent
data class StockChanged(override val sku: String, val units: Int) : CatalogEvent

// Java needs a Class<T> parameter for this. A reified type parameter is available at runtime,
// because the inline function is copied into each call site with the real class.
inline fun <reified T : CatalogEvent> List<CatalogEvent>.only(): List<T> = filter { it is T }.map { it as T }

inline fun <reified T> typeName(): String = T::class.simpleName ?: "?"

// fun <T> typeNameOf(): String = typeName<T>()  // error: cannot use 'T' as reified type parameter. Use a class instead.

fun main() {
    val events = listOf(PriceChanged("SHW-1001", 15_900), StockChanged("SHW-1001", 6), PriceChanged("SHW-2040", 69_000))
    println(events.only<PriceChanged>().map { it.toPaise }) // -> [15900, 69000]
    println(typeName<StockChanged>()) // -> StockChanged

    // The standard library's version of the same idea:
    println(events.filterIsInstance<StockChanged>()) // -> [StockChanged(sku=SHW-1001, units=6)]

    // Erasure still applies to ordinary generic types at runtime: from Any, a List<String> is just a List.
    val anything: Any = listOf("SHW-1001")
    // println(anything is List<String>)  // error: cannot check for instance of erased type 'List<String>'.
    println(anything is List<*>) // -> true
}
