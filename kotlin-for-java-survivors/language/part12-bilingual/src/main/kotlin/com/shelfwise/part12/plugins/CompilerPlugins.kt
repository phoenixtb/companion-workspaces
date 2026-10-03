package com.shelfwise.part12.plugins

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.lang.reflect.Modifier

// Marker annotations wired to the allopen and noarg plugins in build.gradle.kts.
annotation class OpenForFrameworks

annotation class NeedsNoArgConstructor

// What plugin.spring does for @Component, @Service, @Transactional...: the class and its members are open,
// so Spring can subclass it for proxies.
@OpenForFrameworks
class PricingService {
    fun price(sku: String): Long = if (sku == "SHW-1001") 16_500 else 0
}

class PlainService

// What plugin.jpa does for @Entity (since Kotlin 2.3.20, it also opens the class): a synthetic
// no-arg constructor that Kotlin code can't call, but Hibernate can, through reflection.
@NeedsNoArgConstructor
class ProductRow(val sku: String, val pricePaise: Long)

// kotlinx.serialization: the plugin generates a serializer at compile time. No reflection.
@Serializable
data class PriceUpdated(val sku: String, val pricePaise: Long, val currency: String = "INR")

fun main() {
    println(Modifier.isFinal(PricingService::class.java.modifiers)) // -> false
    println(Modifier.isFinal(PlainService::class.java.modifiers)) // -> true

    println(ProductRow::class.java.constructors.map { it.parameterCount }.sorted()) // -> [0, 2]

    val json = Json.encodeToString(PriceUpdated.serializer(), PriceUpdated("SHW-1001", 15_900))
    println(json) // -> {"sku":"SHW-1001","pricePaise":15900}
    val decoded = Json.decodeFromString(PriceUpdated.serializer(), """{"sku":"SHW-2040","pricePaise":69000}""")
    println(decoded) // -> PriceUpdated(sku=SHW-2040, pricePaise=69000, currency=INR)
}
