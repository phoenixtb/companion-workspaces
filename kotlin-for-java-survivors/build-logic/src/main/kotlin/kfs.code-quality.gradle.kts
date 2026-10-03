// Convention: ktlint for formatting, detekt for static analysis.
import org.gradle.api.artifacts.VersionCatalogsExtension

plugins {
    id("org.jlleitschuh.gradle.ktlint")
    id("dev.detekt")
}

val libs = the<VersionCatalogsExtension>().named("libs")

// ktlint: pin the engine (the Gradle plugin's default lags behind). Rules come from .editorconfig.
ktlint {
    version = libs.findVersion("ktlint-cli").get().requiredVersion
}

// detekt 2.0 (alpha) is the line built on the K2 compiler. Only differences from the defaults go in detekt.yml.
detekt {
    buildUponDefaultConfig = true
    config.setFrom("detekt.yml")
}
