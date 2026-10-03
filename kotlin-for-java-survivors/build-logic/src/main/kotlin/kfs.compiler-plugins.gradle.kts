// Convention: the Kotlin compiler plugins Part 12 demonstrates. They live in build-logic so they load
// next to the Kotlin Gradle plugin; a single-module project would write, in its plugins block,
// kotlin("plugin.allopen") version "2.4.20" and so on.
plugins {
    id("org.jetbrains.kotlin.plugin.allopen")
    id("org.jetbrains.kotlin.plugin.noarg")
    id("org.jetbrains.kotlin.plugin.serialization")
    id("org.jetbrains.kotlin.plugin.power-assert")
}
