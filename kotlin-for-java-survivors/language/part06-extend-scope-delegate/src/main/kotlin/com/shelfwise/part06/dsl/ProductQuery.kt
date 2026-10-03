package com.shelfwise.part06.dsl

data class Product(
    val sku: String,
    val name: String,
    val category: String,
    val pricePaise: Long,
    val tags: Set<String> = emptySet(),
)

// Step 1: the marker. Builders annotated with it can't silently reach an outer builder's methods.
@DslMarker
annotation class QueryDsl

// Step 1 (continued): an extension property, so prices read as 200.rupees.
val Int.rupees: Long get() = this * 100L

typealias Criterion = (Product) -> Boolean

// Step 2: the nested builder for alternatives.
@QueryDsl
class AnyOfBuilder {
    internal val alternatives = mutableListOf<Criterion>()

    fun category(name: String) {
        alternatives += { it.category == name }
    }

    fun tagged(tag: String) {
        alternatives += { tag in it.tags }
    }
}

// Step 3: the top-level builder. Each call adds a criterion; nothing runs until the query does.
@QueryDsl
class QueryBuilder {
    private val criteria = mutableListOf<Criterion>()
    private var order: Comparator<Product> = compareBy { it.sku }
    private var limit = Int.MAX_VALUE

    fun category(name: String) {
        criteria += { it.category == name }
    }

    fun tagged(vararg tags: String) {
        criteria += { product -> tags.all { it in product.tags } }
    }

    fun priceBelow(paise: Long) {
        criteria += { it.pricePaise < paise }
    }

    fun anyOf(block: AnyOfBuilder.() -> Unit) {
        val alternatives = AnyOfBuilder().apply(block).alternatives.toList()
        criteria += { product -> alternatives.any { it(product) } }
    }

    fun cheapestFirst() {
        order = compareBy { it.pricePaise }
    }

    fun limit(count: Int) {
        limit = count
    }

    fun build() = ProductQuery(criteria.toList(), order, limit)
}

class ProductQuery(
    private val criteria: List<Criterion>,
    private val order: Comparator<Product>,
    private val limit: Int,
) {
    // invoke: the query object can be called like a function.
    operator fun invoke(products: List<Product>): List<Product> =
        products.filter { product -> criteria.all { it(product) } }.sortedWith(order).take(limit)
}

// Step 4: the entry point. The lambda's receiver is the builder.
fun query(block: QueryBuilder.() -> Unit): ProductQuery = QueryBuilder().apply(block).build()

// Step 5: use it.
fun main() {
    val catalog = listOf(
        Product("SHW-4001", "Penne Rigate 500g", "pasta", 12_000, setOf("gluten-free")),
        Product("SHW-4002", "Fusilli 500g", "pasta", 9_500),
        Product("SHW-4003", "Brown Rice Spaghetti 400g", "pasta", 18_900, setOf("gluten-free", "vegan")),
        Product("SHW-4004", "Quinoa Pasta 250g", "pasta", 24_500, setOf("gluten-free")),
        Product("SHW-5001", "Rice Noodles 200g", "noodles", 8_500, setOf("gluten-free")),
    )

    // "Gluten-free pasta under ₹200?"
    val glutenFreePasta = query {
        category("pasta")
        tagged("gluten-free")
        priceBelow(200.rupees)
        cheapestFirst()
    }
    glutenFreePasta(catalog).forEach { println("${it.sku} ${it.name}") }
    // -> SHW-4001 Penne Rigate 500g
    // -> SHW-4003 Brown Rice Spaghetti 400g

    val noodlesOrVegan = query {
        anyOf {
            category("noodles")
            tagged("vegan")
            // limit(1)  // error: 'fun limit(count: Int): Unit' cannot be called in this context with an implicit receiver. Use an explicit receiver if necessary.
        }
        limit(1)
    }
    println(noodlesOrVegan(catalog).map { it.name }) // -> [Brown Rice Spaghetti 400g]
}
