package com.shelfwise.part08.variance

// Parameters of covariant types get Java wildcards when the type argument is open or an interface.
fun countProducts(products: List<Product>): Int = products.size

fun countSkus(skus: List<String>): Int = skus.size // String is final: no wildcard

// Contravariant parameters get '? super', except for Any (every type is a subtype of Object anyway).
fun auditWith(sink: Sink<Product>) = sink.accept(Product("SHW-1001"))

fun auditAnything(sink: Sink<Any>) = sink.accept("anything")

// Opting out per type: Java callers and reflection see List<Product>.
fun countExact(products: @JvmSuppressWildcards List<Product>): Int = products.size

fun main() {
    println(countProducts(listOf(Product("SHW-1001"))) + countSkus(listOf("SHW-2040"))) // -> 2
    auditWith(AuditSink()) // -> audited SHW-1001
    auditAnything(object : Sink<Any> {
        override fun accept(item: Any) = println("saw $item")
    }) // -> saw anything
    println(countExact(emptyList())) // -> 0
}
