package com.shelfwise.part08.variance

open class Product(val sku: String)
class PerishableProduct(sku: String, val shelfLifeDays: Int) : Product(sku)

// out: T only comes out (return types). A Feed<PerishableProduct> is a Feed<Product>.
interface Feed<out T> {
    fun next(): T
    // fun push(item: T)  // error: type parameter 'T' is declared as 'out' but occurs in 'in' position in type 'T (of interface Feed<out T>)'.
}

// in: T only goes in (parameters). A Sink<Product> is a Sink<PerishableProduct>.
interface Sink<in T> {
    fun accept(item: T)
}

class DairyFeed : Feed<PerishableProduct> {
    override fun next() = PerishableProduct("SHW-3002", 5)
}

class AuditSink : Sink<Product> {
    override fun accept(item: Product) = println("audited ${item.sku}")
}

fun firstOf(feed: Feed<Product>): Product = feed.next()

fun deliver(item: PerishableProduct, sink: Sink<PerishableProduct>) = sink.accept(item)

fun main() {
    println(firstOf(DairyFeed()).sku) // -> SHW-3002
    deliver(PerishableProduct("SHW-3001", 30), AuditSink()) // -> audited SHW-3001

    // The standard library does the same: List<out E> is covariant, MutableList<E> is not.
    val perishables: List<PerishableProduct> = listOf(PerishableProduct("SHW-3002", 5))
    val products: List<Product> = perishables // fine: a read-only List can only hand Products out
    println(products.size) // -> 1

    val shelf: MutableList<PerishableProduct> = mutableListOf()
    // val anyShelf: MutableList<Product> = shelf  // error: initializer type mismatch: expected 'MutableList<Product>', actual 'MutableList<PerishableProduct>'.
    println(shelf.isEmpty()) // -> true
}
