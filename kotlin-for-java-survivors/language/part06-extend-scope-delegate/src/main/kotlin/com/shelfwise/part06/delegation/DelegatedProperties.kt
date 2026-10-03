package com.shelfwise.part06.delegation

import kotlin.properties.Delegates
import kotlin.properties.ReadOnlyProperty
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class Store(val code: String) {
    // lazy: computed on first access, then cached. Thread-safe (synchronized) by default.
    val planogram: List<String> by lazy {
        println("loading planogram for $code")
        listOf("A1: staples", "A2: snacks")
    }
}

class PriceTag(initialPaise: Long) {
    val history = mutableListOf<String>()

    // observable: a callback after every assignment.
    var pricePaise: Long by Delegates.observable(initialPaise) { _, old, new -> history += "$old -> $new" }

    // vetoable: the callback decides whether the assignment happens.
    var discountPercent: Int by Delegates.vetoable(0) { _, _, new -> new in 0..90 }
}

// Map delegation: each property reads the map entry with its own name.
class StoreConfig(values: Map<String, Any?>) {
    val region: String by values
    val openHour: Int by values
}

// A custom delegate: any object with getValue/setValue operators. ReadWriteProperty spells them out.
class Trimmed : ReadWriteProperty<Any?, String> {
    private var value = ""

    override fun getValue(thisRef: Any?, property: KProperty<*>): String = value

    override fun setValue(thisRef: Any?, property: KProperty<*>, value: String) {
        this.value = value.trim()
    }
}

class Listing {
    var title: String by Trimmed()
}

// provideDelegate runs once, when the property is declared: here it registers a column by property name.
open class Table(val name: String) {
    val columns = mutableListOf<String>()

    fun column() = ColumnProvider()

    inner class ColumnProvider {
        operator fun provideDelegate(thisRef: Any?, property: KProperty<*>): ReadOnlyProperty<Any?, String> {
            columns += property.name
            val qualified = "$name.${property.name}"
            return ReadOnlyProperty { _, _ -> qualified }
        }
    }
}

object Products : Table("products") {
    val sku by column()
    val price by column()
}

fun main() {
    // What field-access frameworks (JPA, Gson, Jackson field visibility) see: the delegate, not the value.
    println(Store::class.java.declaredFields.map { it.name }.sorted()) // -> [code, planogram$delegate]

    val pune = Store("PUN-014")
    println("store created") // -> store created
    println(pune.planogram.size)
    // -> loading planogram for PUN-014
    // -> 2
    println(pune.planogram.first()) // -> A1: staples

    val tag = PriceTag(16_500)
    tag.pricePaise = 15_900
    tag.pricePaise = 14_900
    println(tag.history) // -> [16500 -> 15900, 15900 -> 14900]
    tag.discountPercent = 20
    tag.discountPercent = 500 // vetoed: the value stays 20
    println(tag.discountPercent) // -> 20

    val settings = mutableMapOf<String, Any?>("region" to "MH", "openHour" to 7)
    val config = StoreConfig(settings)
    println("${config.region} opens at ${config.openHour}") // -> MH opens at 7
    settings["openHour"] = 9 // the delegate reads the map on every access: no snapshot
    println(config.openHour) // -> 9

    val listing = Listing()
    listing.title = "  Toor Dal 1kg  "
    println("[${listing.title}]") // -> [Toor Dal 1kg]

    println(Products.columns) // -> [sku, price]
    println(Products.sku) // -> products.sku
}
