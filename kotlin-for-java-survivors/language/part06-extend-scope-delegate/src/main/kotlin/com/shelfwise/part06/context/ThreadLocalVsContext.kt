package com.shelfwise.part06.context

import kotlin.concurrent.thread

// Option 1 from the whiteboard: context in a ThreadLocal.
val currentRegion = ThreadLocal<String>()

fun regionFromThreadLocal(): String? = currentRegion.get()

// The alternative: context as a context parameter.
context(region: Region)
fun regionFromContext(): String = region.code

fun main() {
    currentRegion.set("MH")
    var seen: String? = "unset"
    thread { seen = regionFromThreadLocal() }.join() // the new thread has its own, empty slot
    println(seen) // -> null

    context(Region("MH", 5)) {
        thread { seen = regionFromContext() }.join() // the lambda captures the context like any value
    }
    println(seen) // -> MH
}
