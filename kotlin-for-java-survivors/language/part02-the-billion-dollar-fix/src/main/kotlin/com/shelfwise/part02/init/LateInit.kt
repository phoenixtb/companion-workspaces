package com.shelfwise.part02.init

class SupplierClient(val baseUrl: String)

class PriceSyncJob {
    // Set after construction by a framework or a test's @BeforeEach; never null once set.
    lateinit var client: SupplierClient

    // Computed on first access, then cached. Thread-safe by default. (Part 6 covers delegates.)
    val pilotStores: List<String> by lazy {
        println("loading pilot stores")
        listOf("PUN-014", "BLR-003")
    }

    // Genuinely optional: null is a meaningful value ("never ran").
    var lastRunSummary: String? = null

    fun describe(): String =
        if (::client.isInitialized) "client at ${client.baseUrl}" else "no client yet"

    // lateinit var retries: Int  // error: 'lateinit' modifier is not allowed on properties of primitive types.
    // lateinit val region: String  // error: 'lateinit' modifier is allowed only on mutable properties.
}

fun main() {
    val job = PriceSyncJob()
    println(job.describe()) // -> no client yet

    try {
        println(job.client.baseUrl)
    } catch (e: UninitializedPropertyAccessException) {
        println(e.message) // -> lateinit property client has not been initialized
    }

    job.client = SupplierClient("https://supplier.example/api")
    println(job.describe()) // -> client at https://supplier.example/api

    println(job.pilotStores.size) // -> loading pilot stores
    // -> 2
    println(job.pilotStores.first()) // -> PUN-014
    println(job.lastRunSummary ?: "never ran") // -> never ran
}
