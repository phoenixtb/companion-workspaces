plugins {
    // Lets Gradle download a JDK that matches jvmToolchain(...) if none is installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "shelf-labels"
