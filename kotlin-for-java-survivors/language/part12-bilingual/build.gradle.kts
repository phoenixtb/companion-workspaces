import org.jetbrains.kotlin.powerassert.gradle.PowerAssertCompilationFilter

plugins {
    id("kfs.language-part")
    // allopen, noarg, serialization and power-assert. In a Spring app you'd apply kotlin("plugin.spring")
    // and kotlin("plugin.jpa"): allopen and noarg with Spring's and JPA's annotations preconfigured.
    id("kfs.compiler-plugins")
    id("kfs.code-quality") // ktlint and detekt
}

// Mockito's inline mock maker as a Java agent, as Mockito's docs describe: on JDK 21+, self-attaching
// at runtime prints a warning, and future JDKs will refuse it.
val mockitoAgent = configurations.create("mockitoAgent")
dependencies { mockitoAgent(libs.mockito.core) { isTransitive = false } }

tasks.test {
    jvmArgumentProviders.add(CommandLineArgumentProvider { listOf("-javaagent:${mockitoAgent.asPath}") })
    jvmArgs("-XX:+EnableDynamicAgentLoading") // MockK attaches ByteBuddy's agent at runtime: say so explicitly
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    testImplementation(libs.mockk)
    testImplementation(libs.mockito.core)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.kotlinx.coroutines.test)
}

// allopen: classes with this annotation (or annotated with an annotation that has it) are compiled open.
allOpen {
    annotation("com.shelfwise.part12.plugins.OpenForFrameworks")
}

// noarg: classes with this annotation get a synthetic no-argument constructor for frameworks.
noArg {
    annotation("com.shelfwise.part12.plugins.NeedsNoArgConstructor")
}

// Power-assert (Experimental): rewrites calls to these functions so a failure prints a diagram
// of every sub-expression. Tests only: in main, it would append diagrams to production require() messages.
@OptIn(org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi::class)
powerAssert {
    functions = listOf("kotlin.assert", "kotlin.test.assertTrue", "kotlin.test.assertEquals")
    compilationFilter = PowerAssertCompilationFilter.TESTS // the default, spelled out
}

kotlin {
    // explicitApi() // for published libraries: every public declaration needs an explicit visibility and type
    compilerOptions {
        // @JvmExposeBoxed (api/Sku.kt, api/Pricing.kt) is Experimental in 2.4.20.
        optIn.add("kotlin.ExperimentalStdlibApi")
        // Keep parameter names in bytecode for name-based binding. (The Spring Boot plugin sets this for you.)
        javaParameters = true
    }
}
