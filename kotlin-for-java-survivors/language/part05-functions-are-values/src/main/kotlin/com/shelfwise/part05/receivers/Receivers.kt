package com.shelfwise.part05.receivers

class LabelBuilder {
    private val lines = mutableListOf<String>()

    fun line(text: String) {
        lines += text
    }

    fun build(): String = lines.joinToString(" | ")
}

// LabelBuilder.() -> Unit: a lambda with a receiver. Inside it, 'this' is the builder.
fun label(content: LabelBuilder.() -> Unit): String {
    val builder = LabelBuilder()
    builder.content() // call the lambda as if it were a member of 'builder'
    return builder.build()
}

fun main() {
    println(label {
        line("Toor Dal 1kg")
        line("₹159.00")
    }) // -> Toor Dal 1kg | ₹159.00

    // The standard library uses the same trick: buildString's lambda has a StringBuilder receiver.
    println(buildString {
        append("SHW-")
        append(1001)
    }) // -> SHW-1001
}
