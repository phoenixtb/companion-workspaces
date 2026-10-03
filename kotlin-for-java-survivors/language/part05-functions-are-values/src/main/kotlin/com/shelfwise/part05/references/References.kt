package com.shelfwise.part05.references

data class Product(val sku: String, val name: String, val pricePaise: Long)

fun isPremium(product: Product): Boolean = product.pricePaise >= 50_000

class PriceBook(private val overrides: Map<String, Long>) {
    fun priceOf(sku: String): Long = overrides[sku] ?: 0
}

fun main() {
    val products = listOf(
        Product("SHW-1001", "Toor Dal 1kg", 16_500),
        Product("SHW-2040", "Basmati 5kg", 72_000),
    )

    // Top-level function reference: Java's Foo::isPremium without needing a class.
    println(products.filter(::isPremium).map { it.name }) // -> [Basmati 5kg]

    // Property reference: Product::name is a (Product) -> String.
    println(products.map(Product::name)) // -> [Toor Dal 1kg, Basmati 5kg]

    // Bound reference: the receiver is fixed when the reference is created.
    val festiveBook = PriceBook(mapOf("SHW-1001" to 14_900))
    val festivePrice: (String) -> Long = festiveBook::priceOf
    println(festivePrice("SHW-1001")) // -> 14900

    // Constructor reference: ::Product is a (String, String, Long) -> Product.
    val make: (String, String, Long) -> Product = ::Product
    println(make("SHW-3001", "Ghee 500ml", 34_000)) // -> Product(sku=SHW-3001, name=Ghee 500ml, pricePaise=34000)

    // Unbound member reference: the receiver becomes the first parameter.
    val length: (String) -> Int = String::length
    println(length("SHW-1001")) // -> 8
}
