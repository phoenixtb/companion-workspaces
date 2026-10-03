// Convention: JPA entities in Kotlin. plugin.jpa adds no-arg constructors and, since Kotlin 2.3.20,
// opens @Entity, @Embeddable and @MappedSuperclass classes for Hibernate's proxies.
plugins {
    id("org.jetbrains.kotlin.plugin.jpa")
}
