plugins {
    kotlin("jvm") version "2.4.20" // the Kotlin Gradle plugin; adds kotlin-stdlib automatically
    application
}

group = "com.shelfwise"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test")) // kotlin.test on top of JUnit
}

kotlin {
    jvmToolchain(25) // compile and run on JDK 25 (installed or downloaded), whatever JDK launched Gradle
}

application {
    mainClass = "com.shelfwise.MainKt" // the class the compiler generates for Main.kt
}

tasks.test {
    useJUnitPlatform()
}

