package com.shelfwise.part09.annotations

// Shaped like a Bean Validation annotation: it may target parameters, fields and getters, but not Kotlin properties.
@Target(AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FIELD, AnnotationTarget.PROPERTY_GETTER)
@Retention(AnnotationRetention.RUNTIME)
annotation class MinPaise(val value: Long)

class PriceUpdate(
    @MinPaise(100) val defaulted: Long, // 2.4.0 default: the parameter, and the field
    @field:MinPaise(100) val onField: Long, // only the field
    @get:MinPaise(100) val onGetter: Long, // only the getter
    @all:MinPaise(100) val everywhere: Long, // @all (Stable since 2.4.0): parameter, field and getter
)

fun main() {
    val type = PriceUpdate::class.java
    val names = listOf("defaulted", "onField", "onGetter", "everywhere")
    val parameters = type.constructors.single().parameters
    for ((index, name) in names.withIndex()) {
        val getter = "get" + name.replaceFirstChar { it.uppercase() }
        val places = buildList {
            if (parameters[index].isAnnotationPresent(MinPaise::class.java)) add("parameter")
            if (type.getDeclaredField(name).isAnnotationPresent(MinPaise::class.java)) add("field")
            if (type.getMethod(getter).isAnnotationPresent(MinPaise::class.java)) add("getter")
        }
        println("$name: $places")
    }
    // -> defaulted: [parameter, field]
    // -> onField: [field]
    // -> onGetter: [getter]
    // -> everywhere: [parameter, field, getter]
}
