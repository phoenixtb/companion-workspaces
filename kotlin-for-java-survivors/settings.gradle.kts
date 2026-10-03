// One multi-project build for the whole series. Open this folder (not the
// monorepo root) in IntelliJ IDEA.
pluginManagement {
    includeBuild("build-logic")
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    // Downloads a matching JDK when no installed one fits jvmToolchain(25).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "kotlin-for-java-survivors"

// Act I and II: one module per part, plus the output-checking test kit.
include(":language:testkit")
include(":language:part01-life-after-semicolons")
include(":language:part02-the-billion-dollar-fix")
include(":language:part03-classes-without-ceremony")
include(":language:part04-data-sealed-and-value")
include(":language:part05-functions-are-values")
include(":language:part06-extend-scope-delegate")
include(":language:part07-collections-without-collectors")
include(":language:part08-generics-without-wildcards")
include(":language:part09-errors-without-checked-exceptions")
include(":language:part10-suspend-your-disbelief")
include(":language:part11-flowing-data")
include(":language:part12-bilingual")

// Act III: the services.
include(":services:product-catalog-service")
