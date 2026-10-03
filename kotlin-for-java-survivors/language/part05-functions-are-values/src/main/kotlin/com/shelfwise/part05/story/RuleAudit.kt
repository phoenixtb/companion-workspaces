package com.shelfwise.part05.story

class PriceRule(val name: String, val minPaise: Long, val percentOff: Int)

// Kabir's version: 'return' meant "skip this rule", as it would inside a Java lambda.
fun auditBroken(pricePaise: Long, rules: List<PriceRule>, log: MutableList<String>) {
    rules.forEach { rule ->
        if (pricePaise < rule.minPaise) return // returns from auditBroken, not from the lambda
        log += "${rule.name}: -${rule.percentOff}%"
    }
    log += "audited ${rules.size} rules"
}

// The fix: a labelled return leaves only the lambda, like 'continue' in a loop.
fun audit(pricePaise: Long, rules: List<PriceRule>, log: MutableList<String>) {
    rules.forEach { rule ->
        if (pricePaise < rule.minPaise) return@forEach
        log += "${rule.name}: -${rule.percentOff}%"
    }
    log += "audited ${rules.size} rules"
}

fun main() {
    val rules = listOf(PriceRule("BULK", 50_000, 5), PriceRule("FESTIVE", 10_000, 10))

    val broken = mutableListOf<String>()
    auditBroken(20_000, rules, broken)
    println(broken) // -> []

    val fixed = mutableListOf<String>()
    audit(20_000, rules, fixed)
    println(fixed) // -> [FESTIVE: -10%, audited 2 rules]
}
