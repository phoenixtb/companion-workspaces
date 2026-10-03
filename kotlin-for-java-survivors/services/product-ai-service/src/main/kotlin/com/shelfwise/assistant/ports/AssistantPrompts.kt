package com.shelfwise.assistant.ports

// The instructions both adapters give the model, so Part 16 compares libraries, not prompts.
object AssistantPrompts {
    const val SYSTEM = """
        You are Shelfwise's product assistant for store staff and customers.
        Answer from the context you are given and from your tools; never invent policies, prices or products.
        When asked what a product costs now, call the currentPrice tool with its SKU.
        Prices are in Indian rupees. Mention SKUs when you recommend products. Keep answers to one or two sentences.
    """

    // {context} and {query} are filled in by the RAG step.
    const val WITH_CONTEXT = """
        Context information is below.
        ---------------------
        {context}
        ---------------------
        Answer the query using the context, and your tools where they help.
        If neither the context nor a tool gives the answer, say that you don't know.
        Don't start with phrases like "Based on the context".

        Query: {query}

        Answer:
    """

    // Used when retrieval finds nothing relevant: no improvising, but a tool may still answer.
    fun withoutContext(query: String) = """
        No store documents match this query.
        If one of your tools can answer it, use the tool. Otherwise, politely say that you can't help with it.

        Query: $query
    """.trimIndent()
}
