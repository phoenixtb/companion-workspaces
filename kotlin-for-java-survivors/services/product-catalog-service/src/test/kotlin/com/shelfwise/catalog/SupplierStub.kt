package com.shelfwise.catalog

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.Executors

// A stand-in for the suppliers' quote API, on the JDK's built-in HTTP server: SUP-003 is too slow
// for the 500 ms budget, SUP-005 is down, the rest quote 16,000 paise plus a few rupees each.
object SupplierStub {
    private val server: HttpServer = HttpServer.create(InetSocketAddress("localhost", 0), 0).apply {
        executor = Executors.newVirtualThreadPerTaskExecutor()
        createContext("/suppliers/") { exchange ->
            val (_, _, code, _, sku) = exchange.requestURI.path.split('/')
            when (code) {
                "SUP-003" -> Thread.sleep(2_000)
                "SUP-005" -> exchange.sendResponseHeaders(503, -1).also { exchange.close() }
            }
            if (code != "SUP-005") {
                val paise = 16_000 + code.takeLast(1).toInt() * 100
                val body = """{"supplier":"$code","sku":"$sku","pricePaise":$paise}""".encodeToByteArray()
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
        }
        start()
    }

    val baseUrl = "http://localhost:${server.address.port}"
}
