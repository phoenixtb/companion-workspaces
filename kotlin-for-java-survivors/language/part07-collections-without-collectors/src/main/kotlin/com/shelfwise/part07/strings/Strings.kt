package com.shelfwise.part07.strings

fun main() {
    // split takes a literal delimiter, not a regex as in Java.
    println("SHW.1001.MH".split(".")) // -> [SHW, 1001, MH]
    println("SHW.1001.MH".split(Regex("\\d+"))) // -> [SHW., .MH]

    // And it keeps trailing empty strings, which Java's split drops.
    println("dal,rice,,".split(",")) // -> [dal, rice, , ]

    // trim() removes Unicode whitespace, such as the no-break spaces web forms like to send.
    // Java's trim() only removes characters up to U+0020, and strip() doesn't count U+00A0.
    val fromForm = "\u00A0Toor Dal\u00A0"
    println(fromForm.trim().length) // -> 8
    @Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN") // deliberately calling Java's String.trim()
    println((fromForm as java.lang.String).trim().length) // -> 10

    // Small helpers that replace a utility class.
    val code = "SHW-1001:MH"
    println(code.substringBefore(":")) // -> SHW-1001
    println("12a".toIntOrNull() ?: -1) // -> -1

    // Regex: find, findAll, destructured groups.
    val sku = Regex("""([A-Z]{3})-(\d{4})""")
    val match = sku.find("label: SHW-1001, alt SHW-2040")
    val (prefix, number) = match!!.destructured
    println("$prefix / $number") // -> SHW / 1001
    println(sku.findAll("SHW-1001 SHW-2040").map { it.value }.toList()) // -> [SHW-1001, SHW-2040]
    println("SHW-1001" matches sku) // -> true
}
