package com.shelfwise.assistant

import org.junit.jupiter.api.Test
import org.springframework.ai.tool.annotation.Tool
import org.springframework.ai.tool.annotation.ToolParam
import org.springframework.ai.tool.method.MethodToolCallbackProvider
import kotlin.test.assertTrue

// The JSON schema the model sees for a @Tool comes from the Kotlin signature, nullability included,
// unless an explicit @ToolParam says otherwise: its required() defaults to true and wins.
class ToolSchemaTest {
    class BareTools {
        @Tool(description = "Units on the shelf, chain-wide or in one store")
        fun stock(sku: String, store: String?): Int = 0
    }

    class DescribedTools {
        @Tool(description = "Units on the shelf, chain-wide or in one store")
        fun stock(sku: String, @ToolParam(description = "A store code, or none for all stores") store: String?): Int = 0
    }

    class OptionalTools {
        @Tool(description = "Units on the shelf, chain-wide or in one store")
        fun stock(sku: String, @ToolParam(description = "A store code, or none for all stores", required = false) store: String?): Int = 0
    }

    private fun schemaOf(tools: Any) =
        MethodToolCallbackProvider.builder().toolObjects(tools).build().toolCallbacks.single().toolDefinition.inputSchema()

    @Test
    fun `without @ToolParam, a nullable parameter is optional`() {
        assertTrue(""""required" : [ "sku" ]""" in schemaOf(BareTools()))
    }

    @Test
    fun `@ToolParam makes it required again, unless required = false`() {
        assertTrue(""""required" : [ "sku", "store" ]""" in schemaOf(DescribedTools()))
        assertTrue(""""required" : [ "sku" ]""" in schemaOf(OptionalTools()))
    }
}
