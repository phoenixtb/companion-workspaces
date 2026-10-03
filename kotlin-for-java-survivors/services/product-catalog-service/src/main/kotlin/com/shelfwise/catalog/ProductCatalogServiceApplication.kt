package com.shelfwise.catalog

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ProductCatalogServiceApplication

fun main(args: Array<String>) {
    runApplication<ProductCatalogServiceApplication>(*args)
}
