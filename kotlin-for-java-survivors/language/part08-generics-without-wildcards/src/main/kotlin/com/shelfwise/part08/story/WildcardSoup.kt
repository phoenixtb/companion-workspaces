package com.shelfwise.part08.story

import com.shelfwise.part08.legacy.Entity
import com.shelfwise.part08.legacy.LegacyRepository
import java.util.function.Predicate

class Store(private val code: String, val city: String) : Entity<String> {
    override fun id() = code
}

class StoreRepository(private val stores: List<Store>) : LegacyRepository<Store, String> {
    // Kotlin shows the Java wildcards as projections: List<out Store>, Predicate<in Store>, Comparator<in Store>.
    override fun findAll(filter: Predicate<in Store>, order: Comparator<in Store>): List<out Store> =
        stores.filter { filter.test(it) }.sortedWith(order)
}

fun main() {
    val repo = StoreRepository(listOf(Store("PUN-014", "Pune"), Store("MUM-002", "Mumbai"), Store("PUN-021", "Pune")))

    // A Comparator<Entity<String>> is accepted where a Comparator<in Store> is expected.
    val byId: Comparator<Entity<String>> = compareBy { it.id() }
    val pune = repo.findAll({ it.city == "Pune" }, byId)
    println(pune.map { it.id() }) // -> [PUN-014, PUN-021]
}
