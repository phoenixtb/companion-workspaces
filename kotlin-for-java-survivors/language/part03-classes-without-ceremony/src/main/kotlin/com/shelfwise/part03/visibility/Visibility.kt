package com.shelfwise.part03.visibility

open class PricingRule {
    // protected = subclasses only. No package access, unlike Java.
    protected fun marginPercent(): Int = 18

    // internal = visible anywhere in the same module (Gradle source set), nowhere else.
    internal fun auditCode(): String = "RULE-${marginPercent()}"
}

class FestivalRule : PricingRule() {
    fun summary(): String = "festival margin ${marginPercent()}%" // fine: a subclass
}

// Same package, not a subclass. In Java, a protected member would be accessible here.
class PriceReport {
    fun audit(rule: PricingRule): String = rule.auditCode() // internal: same module, fine
    // fun peek(rule: PricingRule) = rule.marginPercent()  // error: cannot access 'fun marginPercent(): Int': it is protected in 'com.shelfwise.part03.visibility.PricingRule'.
}

// private at the top level = visible in this file only.
private fun secretFormula(): Int = 42

fun main() {
    println(FestivalRule().summary()) // -> festival margin 18%
    println(PriceReport().audit(PricingRule())) // -> RULE-18
    println(secretFormula()) // -> 42
}
