package com.shelfwise.catalog.product

// One sealed hierarchy for every business failure, so the web layer's mapping is an exhaustive when.
sealed class CatalogException(message: String) : RuntimeException(message)

class ProductNotFound(val sku: String) : CatalogException("No product with SKU $sku")

class DuplicateSku(val sku: String) : CatalogException("A product with SKU $sku already exists")

class UnknownSupplier(val code: String) : CatalogException("No supplier with code $code")
