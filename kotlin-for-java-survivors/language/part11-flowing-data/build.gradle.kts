plugins {
    id("kfs.language-part")
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.reactor) // mono { }, flux { }, awaitSingle(), asFlow(); brings reactor-core
    implementation(libs.kotlinx.coroutines.test) // runTest and virtual time, used by examples and tests
    testImplementation(libs.kotlinx.coroutines.test)
}
