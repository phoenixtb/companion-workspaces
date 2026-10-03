package com.shelfwise.catalog

import com.shelfwise.catalog.product.Category
import com.shelfwise.catalog.product.NewProduct
import java.io.IOException
import java.io.Reader

fun newProduct(sku: String) = NewProduct(
    sku = sku,
    name = "Masala Oats 500 g",
    description = "Rolled oats with a spice mix.",
    category = Category.BREAKFAST,
    pricePaise = 14_000,
    supplierCode = "SUP-002",
    dietaryTags = setOf("vegetarian"),
)

// A supplier's price list whose connection drops after the given text has been read.
class FailingAfter(private val text: String) : Reader() {
    private var served = false

    override fun read(buffer: CharArray, offset: Int, length: Int): Int {
        if (served) throw IOException("connection reset by supplier")
        served = true
        text.toCharArray().copyInto(buffer, offset)
        return text.length
    }

    override fun close() {}
}
