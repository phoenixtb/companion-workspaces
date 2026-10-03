package com.shelfwise.assistant.tools

import dev.langchain4j.agent.tool.P
import org.springframework.ai.tool.annotation.Tool
import org.springframework.ai.tool.annotation.ToolParam
import dev.langchain4j.agent.tool.Tool as Lc4jTool
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
// Both libraries' annotations on one method: an import alias keeps the two @Tool names apart.
@Component
class CatalogTools(private val catalog: CatalogClient) {
    @Tool(description = DESCRIPTION)
    @Lc4jTool(DESCRIPTION)
    fun currentPrice(@ToolParam(description = SKU) @P(SKU) sku: String): String =
        try {
            catalog.product(sku).let { "${it.name} (${it.sku}) costs ${it.price}" }
        } catch (e: HttpClientErrorException.NotFound) {
            "There is no product with SKU $sku"
        }

    companion object {
        const val DESCRIPTION = "Get the current price of a Shelfwise product from the catalog, by SKU. Use it when asked what something costs now."
        const val SKU = "The product SKU, for example SHW-1001"
    }
}
