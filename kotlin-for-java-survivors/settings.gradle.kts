// One multi-project build for the whole series. Open this folder (not the
// monorepo root) in IntelliJ IDEA.
pluginManagement {
    includeBuild("build-logic")
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
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
