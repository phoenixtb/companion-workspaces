plugins {
    id("kfs.language-part")
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
}

tasks.withType<Test>().configureEach {
    // Coroutine debug mode (debugging/Debugging.kt): coroutine names in thread names, stack-trace recovery.
    systemProperty("kotlinx.coroutines.debug", "on")
}
