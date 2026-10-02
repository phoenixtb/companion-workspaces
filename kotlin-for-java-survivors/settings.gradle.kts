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
