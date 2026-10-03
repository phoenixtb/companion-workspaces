package com.shelfwise.part09.interop

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.lang.reflect.UndeclaredThrowableException

interface PriceSource {
    fun load(): List<String> // no @Throws: Java sees no 'throws IOException'
}

interface CheckedPriceSource {
    @Throws(IOException::class)
    fun load(): List<String> // Java implementations may declare 'throws IOException'
}

class SupplierFeed : PriceSource {
    override fun load(): List<String> = throw IOException("feed offline")
}

fun main() {
    // A JDK dynamic proxy (what Spring AOP uses for interfaces) may only rethrow exceptions
    // the interface method declares. An undeclared checked exception gets wrapped.
    val target = SupplierFeed()
    val proxy = Proxy.newProxyInstance(PriceSource::class.java.classLoader, arrayOf(PriceSource::class.java)) { _, method, args ->
        try {
            method.invoke(target, *(args ?: emptyArray()))
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
    } as PriceSource
    try {
        proxy.load()
    } catch (e: UndeclaredThrowableException) {
        println("${e::class.simpleName} caused by ${e.cause}") // -> UndeclaredThrowableException caused by java.io.IOException: feed offline
    }

    // A Java class implements CheckedPriceSource with 'throws IOException' (see JavaFeed.java).
    try {
        com.shelfwise.part09.javacaller.JavaFeed().load()
    } catch (e: IOException) {
        println("Java implementation threw: ${e.message}") // -> Java implementation threw: export locked
    }
}
