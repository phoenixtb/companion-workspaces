package com.shelfwise.catalog

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.PropagationContextElement
import org.springframework.test.web.servlet.client.RestTestClient
import org.springframework.test.web.servlet.client.expectBody
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import kotlin.test.assertEquals

// Does the MDC request id survive the thread hops of a coroutine? With spring.reactor.context-propagation
// set to auto (application.yaml): inside the handler's coroutine, yes; in a coroutine of your own, only
// if you pass PropagationContextElement().
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureRestTestClient
@Import(TestcontainersConfiguration::class, ContextPropagationTest.Probe::class)
class ContextPropagationTest(@Autowired private val client: RestTestClient) {
    @RestController
    class Probe {
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default) // as a bean-owned scope would be

        @GetMapping("/test/mdc/handler")
        suspend fun handler() = withContext(Dispatchers.IO) { delay(1); MDC.get("requestId") ?: "missing" }

        @GetMapping("/test/mdc/own-scope")
        suspend fun ownScope() = scope.async { delay(1); MDC.get("requestId") ?: "missing" }.await()

        @GetMapping("/test/mdc/own-scope-propagated")
        suspend fun ownScopePropagated() =
            scope.async(PropagationContextElement()) { delay(1); MDC.get("requestId") ?: "missing" }.await()
    }

    private fun requestIdSeenBy(path: String) = client.get().uri(path).header("X-Request-Id", "req-42").exchange()
        .expectStatus().isOk.expectBody<String>().returnResult().responseBody

    @Test
    fun `the handler's coroutine keeps the request id across dispatchers`() =
        assertEquals("req-42", requestIdSeenBy("/test/mdc/handler"))

    @Test
    fun `a coroutine from another scope starts without it`() =
        assertEquals("missing", requestIdSeenBy("/test/mdc/own-scope"))

    @Test
    fun `PropagationContextElement carries it into that scope`() =
        assertEquals("req-42", requestIdSeenBy("/test/mdc/own-scope-propagated"))
}
