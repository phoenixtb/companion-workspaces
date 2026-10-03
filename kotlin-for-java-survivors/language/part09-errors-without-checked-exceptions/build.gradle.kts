plugins {
    id("kfs.language-part")
}

dependencies {
    // Full Kotlin reflection (reflection/Reflection.kt). About 3 MB; most code doesn't need it.
    implementation(libs.kotlin.reflect)
}

kotlin {
    compilerOptions {
        // Experimental in 2.4.20: the unused return value checker (returnvalues/ReturnValues.kt).
        // 'check' reports ignored results of declarations marked @MustUseReturnValues.
        freeCompilerArgs.add("-Xreturn-value-checker=check")
    }
}
