package com.shelfwise.catalog.web

import io.micrometer.context.ContextRegistry
import io.micrometer.context.integration.Slf4jThreadLocalAccessor
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.MDC
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID

// Puts a request id into SLF4J's MDC for every log line of the request, and echoes it back.
@Component
class RequestIdFilter : OncePerRequestFilter() {
    init {
        // Teach Micrometer's context propagation about this MDC key, so it can be carried across threads.
        ContextRegistry.getInstance().registerThreadLocalAccessor(Slf4jThreadLocalAccessor(KEY))
    }

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        val requestId = request.getHeader(HEADER) ?: UUID.randomUUID().toString()
        MDC.put(KEY, requestId)
        response.setHeader(HEADER, requestId)
        try {
            chain.doFilter(request, response)
        } finally {
            MDC.remove(KEY)
        }
    }

    companion object {
        const val KEY = "requestId"
        const val HEADER = "X-Request-Id"
    }
}
