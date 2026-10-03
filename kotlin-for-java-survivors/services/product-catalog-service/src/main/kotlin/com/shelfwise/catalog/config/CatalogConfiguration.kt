package com.shelfwise.catalog.config

import org.springframework.beans.factory.BeanRegistrarDsl
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.transaction.annotation.RollbackOn
import java.time.Clock

// Spring rolls back on unchecked exceptions only. Kotlin has no checked exceptions, so a Kotlin
// function can throw IOException without anyone noticing, and the transaction would commit (Part 9).
@Configuration(proxyBeanMethods = false)
@EnableTransactionManagement(rollbackOn = RollbackOn.ALL_EXCEPTIONS)
@Import(CatalogBeans::class)
class CatalogConfiguration

// Programmatic bean registration with Spring Framework 7's Kotlin DSL.
class CatalogBeans : BeanRegistrarDsl({
    registerBean<Clock> { Clock.systemUTC() }
})
