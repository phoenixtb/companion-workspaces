package com.shelfwise.part08.events

import kotlin.reflect.KClass

sealed interface CatalogEvent {
    val sku: String
}

data class ProductAdded(override val sku: String, val name: String) : CatalogEvent
data class PriceChanged(override val sku: String, val toPaise: Long) : CatalogEvent
data class StockChanged(override val sku: String, val units: Int) : CatalogEvent

// Step 1: an envelope only hands its event out, so it is covariant.
// An Envelope<PriceChanged> is an Envelope<CatalogEvent>.
data class Envelope<out E : CatalogEvent>(val key: String, val sequence: Long, val event: E)

// Step 2: a handler only takes envelopes in, so it is contravariant.
// A Handler<CatalogEvent> can stand in for a Handler<PriceChanged>.
fun interface Handler<in E : CatalogEvent> {
    fun handle(envelope: Envelope<E>)
}

// Step 3: a router keyed by event class. The class token is explicit in register(),
// and comes from a reified type parameter in on().
class EventRouter {
    private val handlers = mutableMapOf<KClass<*>, MutableList<Handler<*>>>()

    fun <E : CatalogEvent> register(type: KClass<E>, handler: Handler<E>) {
        handlers.getOrPut(type) { mutableListOf() } += handler
    }

    inline fun <reified E : CatalogEvent> on(handler: Handler<E>) = register(E::class, handler)

    fun dispatch(envelope: Envelope<CatalogEvent>) {
        for (handler in handlers[envelope.event::class].orEmpty()) {
            @Suppress("UNCHECKED_CAST") // safe: handlers are registered under their event's own class
            (handler as Handler<CatalogEvent>).handle(envelope)
        }
    }
}

// Step 4: the repository from the whiteboard, with variance declared once.
interface ReadRepository<out T : Any, in ID> {
    fun findById(id: ID): T?

    fun findAll(filter: (T) -> Boolean = { true }, order: Comparator<in T>? = null): List<T>
}

// Writing takes T in, so T is invariant here: split the interface, as List and MutableList do.
interface Repository<T : Any, in ID> : ReadRepository<T, ID> {
    fun save(entity: T)
}

class InMemoryRepository<T : Any, ID>(items: List<T>, private val idOf: (T) -> ID) : Repository<T, ID> {
    private val byId = items.associateBy(idOf).toMutableMap()

    override fun findById(id: ID): T? = byId[id]

    override fun save(entity: T) {
        byId[idOf(entity)] = entity
    }

    override fun findAll(filter: (T) -> Boolean, order: Comparator<in T>?): List<T> {
        val matching = byId.values.filter(filter)
        return if (order == null) matching else matching.sortedWith(order)
    }
}

data class Product(val sku: String, val name: String, val pricePaise: Long)

fun main() {
    val router = EventRouter()
    router.on<PriceChanged> { println("reprice ${it.event.sku} -> ${it.event.toPaise}") }
    router.on<StockChanged> { println("stock ${it.event.sku}: ${it.event.units}") }

    // One audit handler for every event type, thanks to 'in E'.
    val audit = Handler<CatalogEvent> { println("audit #${it.sequence} ${it.event.sku}") }
    router.on<ProductAdded>(audit)
    router.on<PriceChanged>(audit)

    // A handler registered for the supertype never fires: dispatch looks up the exact runtime class.
    router.on<CatalogEvent> { println("never printed") }

    val added: Envelope<ProductAdded> = Envelope("SHW-1001", 1, ProductAdded("SHW-1001", "Toor Dal 1kg"))
    val priced: Envelope<PriceChanged> = Envelope("SHW-1001", 2, PriceChanged("SHW-1001", 15_900))
    val stocked: Envelope<StockChanged> = Envelope("SHW-1001", 3, StockChanged("SHW-1001", 6))
    // dispatch takes an Envelope<CatalogEvent>; an Envelope<PriceChanged> fits only because of 'out E'.
    listOf(added, priced, stocked).forEach(router::dispatch)
    // -> audit #1 SHW-1001
    // -> reprice SHW-1001 -> 15900
    // -> audit #2 SHW-1001
    // -> stock SHW-1001: 6

    val products: Repository<Product, String> = InMemoryRepository(
        listOf(Product("SHW-2040", "Basmati 5kg", 72_000), Product("SHW-1001", "Toor Dal 1kg", 16_500)),
    ) { it.sku }
    // A Comparator<Any> works where a Comparator<in Product> is expected.
    val byText: Comparator<Any> = compareBy { it.toString() }
    println(products.findAll(order = byText).map { it.sku }) // -> [SHW-1001, SHW-2040]
    products.save(Product("SHW-2040", "Basmati 5kg (new pack)", 69_000))
    println(products.findById("SHW-2040")?.name) // -> Basmati 5kg (new pack)

    // A read-only view of it is a ReadRepository<Any, String>, thanks to 'out T'.
    val anything: ReadRepository<Any, String> = products
    println(anything.findAll().size) // -> 2
}
