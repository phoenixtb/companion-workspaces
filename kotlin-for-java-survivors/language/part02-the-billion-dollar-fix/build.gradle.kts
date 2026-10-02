plugins {
    id("kfs.language-part")
}

dependencies {
    // The "supplier SDK" in src/main/java uses JSpecify annotations in its v2 package.
    implementation(libs.jspecify)
}
