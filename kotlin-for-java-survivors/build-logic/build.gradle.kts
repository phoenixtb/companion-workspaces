plugins {
    `kotlin-dsl`
}

dependencies {
    implementation(libs.kotlin.gradle.plugin)
    // Compiler plugins on the same classpath as KGP, so modules apply them by id without a version.
    implementation(libs.kotlin.allopen)
    implementation(libs.kotlin.noarg)
    implementation(libs.kotlin.serialization)
    implementation(libs.kotlin.power.assert)
    // Lint plugins, for the same reason: each would otherwise bring its own copy of KGP.
    implementation(libs.ktlint.gradle.plugin)
    implementation(libs.detekt.gradle.plugin)
}
