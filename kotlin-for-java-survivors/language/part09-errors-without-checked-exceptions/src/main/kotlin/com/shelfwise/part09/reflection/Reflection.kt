package com.shelfwise.part09.reflection

import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

data class Product(val sku: String, val name: String, val pricePaise: Long)

fun main() {
    val product = Product("SHW-1001", "Toor Dal 1kg", 16_500)

    // Without kotlin-reflect: class references, names, and property references you can call.
    val type = Product::class
    println(type.simpleName) // -> Product
    println(type.java.name) // -> com.shelfwise.part09.reflection.Product
    val nameProperty: KProperty1<Product, String> = Product::name
    println("${nameProperty.name} = ${nameProperty.get(product)}") // -> name = Toor Dal 1kg

    // With kotlin-reflect on the classpath: full introspection of Kotlin declarations.
    val properties = type.memberProperties.sortedBy { it.name }.map { "${it.name}: ${it.returnType}" }
    println(properties) // -> [name: kotlin.String, pricePaise: kotlin.Long, sku: kotlin.String]
    println(type.isData) // -> true
}
