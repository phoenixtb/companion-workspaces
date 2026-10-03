// Convention: a Spring Boot 4.1 service in Kotlin. It is what start.spring.io generates for Kotlin,
// minus what Kotlin 2.4.20 made redundant. Plugin versions come from build-logic's dependencies.
import org.gradle.api.artifacts.VersionCatalogsExtension

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

val libs = the<VersionCatalogsExtension>().named("libs")

// Boot 4.1.1's BOM manages Kotlin 2.3.21, but the Boot plugin aligns its kotlin.version with the Kotlin
// plugin applied (2.4.20) and adds -java-parameters. kotlinx.coroutines gets no such help: the BOM pins
// 1.10.2, so the property moves it to the version the series uses.
extra["kotlin-coroutines.version"] = libs.findVersion("kotlinx-coroutines").get().requiredVersion

kotlin {
    jvmToolchain(libs.findVersion("jdk").get().requiredVersion.toInt())
}

// Spring Boot's test support initialises Mockito's inline mock maker, which on JDK 21+ self-attaches
// an agent with a warning (Part 12). Pass it as -javaagent instead; Boot's BOM supplies the version.
val mockitoAgent = configurations.create("mockitoAgent")

dependencies {
    "mockitoAgent"("org.mockito:mockito-core") { isTransitive = false }
    "implementation"("org.jetbrains.kotlin:kotlin-reflect")
    "testImplementation"("org.jetbrains.kotlin:kotlin-test-junit5")
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgumentProviders.add(CommandLineArgumentProvider { listOf("-javaagent:${mockitoAgent.asPath}") })
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
