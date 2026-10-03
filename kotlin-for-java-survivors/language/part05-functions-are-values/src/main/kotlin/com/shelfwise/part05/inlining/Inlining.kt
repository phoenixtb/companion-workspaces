package com.shelfwise.part05.inlining

// Not inline: the lambda becomes a Function1 object, and the Long travels boxed through invoke(Object).
fun applyBoxed(pricePaise: Long, rule: (Long) -> Long): Long = rule(pricePaise)

// inline: the body and the lambda are copied into the caller. No object, no boxing.
inline fun applyInline(pricePaise: Long, rule: (Long) -> Long): Long = rule(pricePaise)

fun callBoxed(): Long = applyBoxed(16_500) { it - 600 }

fun callInline(): Long = applyInline(16_500) { it - 600 }

// The typical inline helper: behaviour around a block, with no allocation per call.
inline fun <T> audited(log: MutableList<String>, label: String, block: () -> T): T {
    log += "start $label"
    val result = block()
    log += "end $label = $result"
    return result
}

// noinline: 'describe' is stored for later, so it must stay a real object.
inline fun register(registry: MutableList<() -> String>, enabled: () -> Boolean, noinline describe: () -> String) {
    if (enabled()) registry += describe
}
// Without noinline:
// inline fun registerAll(registry: MutableList<() -> String>, describe: () -> String) { registry += describe }
//   error: illegal usage of inline parameter 'describe: () -> String'. Add 'noinline' modifier to the parameter declaration.

// crossinline: 'action' runs inside another object, so it must not 'return' from the caller.
inline fun deferred(crossinline action: () -> Unit): Runnable = Runnable { action() }
// Without crossinline:
// inline fun deferredAll(action: () -> Unit): Runnable = Runnable { action() }
//   error: cannot inline 'action: () -> Unit' here: it might contain non-local returns. Add 'crossinline' modifier to parameter declaration 'action: () -> Unit'.

class Pricing(private val vatPercent: Int) {
    // inline fun withVat(pricePaise: Long, round: (Long) -> Long): Long = round(pricePaise * (100 + vatPercent) / 100)  // error: public-API inline function cannot access non-public-API property.
    fun withVat(pricePaise: Long): Long = pricePaise * (100 + vatPercent) / 100
}

fun main() {
    println(callBoxed()) // -> 15900
    println(callInline()) // -> 15900

    val log = mutableListOf<String>()
    val total = audited(log, "basket") { 16_500L + 3_050L }
    println(total) // -> 19550
    println(log) // -> [start basket, end basket = 19550]

    val registry = mutableListOf<() -> String>()
    register(registry, { true }) { "FESTIVE 10%" }
    register(registry, { false }) { "BULK 5%" }
    println(registry.map { it() }) // -> [FESTIVE 10%]

    val task = deferred { println("index rebuilt") }
    task.run() // -> index rebuilt

    println(Pricing(5).withVat(10_000)) // -> 10500
}
