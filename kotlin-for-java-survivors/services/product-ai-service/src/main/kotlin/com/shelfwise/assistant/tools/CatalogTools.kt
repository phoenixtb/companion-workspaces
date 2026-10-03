package com.shelfwise.assistant.tools

import org.springframework.ai.tool.annotation.Tool
import org.springframework.ai.tool.annotation.ToolParam
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.service.annotation.GetExchange

data class CatalogProduct(val sku: String, val name: String, val price: String)

// The catalog's REST API, as an HTTP interface (Part 14). Blocking is fine here: tools run on the
// thread that serves the request, a virtual thread.
interface CatalogClient {
    @GetExchange("/api/products/{sku}")
    fun product(@PathVariable sku: String): CatalogProduct
}

// Indexed prices can be minutes old; a tool lets the model ask the catalog for the price right now.
@Component
class CatalogTools(private val catalog: CatalogClient) {
    @Tool(description = "Get the current price of a Shelfwise product from the catalog, by SKU. Use it when asked what something costs now.")
    fun currentPrice(@ToolParam(description = "The product SKU, for example SHW-1001") sku: String): String =
        try {
            catalog.product(sku).let { "${it.name} (${it.sku}) costs ${it.price}" }
        } catch (e: HttpClientErrorException.NotFound) {
            "There is no product with SKU $sku"
        }
}
