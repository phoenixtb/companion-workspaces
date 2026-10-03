@file:JvmName("Pricing") // the facade class Java sees, instead of PricingKt

package com.shelfwise.part12.api

// Takes a value class: the JVM name is mangled (shelfLabel-<hash>), unusable from Java.
// Functions outside the class need their own @JvmExposeBoxed: this adds shelfLabel(Sku, long).
@JvmExposeBoxed
fun shelfLabel(sku: Sku, pricePaise: Long): String = "$sku ₹${pricePaise / 100}"

// The other fix: an explicit JVM name. Java then passes the raw String, and Sku's init never runs.
@JvmName("shelfLabelUnchecked")
fun shelfLabelRaw(sku: Sku, pricePaise: Long): String = "$sku ₹${pricePaise / 100}"

// An extension function: Pricing.formatPaise(long) from Java.
fun Long.formatPaise(): String = "₹${this / 100}.${(this % 100).toString().padStart(2, '0')}"
