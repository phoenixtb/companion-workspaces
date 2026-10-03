plugins {
    id("kfs.language-part")
}

kotlin {
    compilerOptions {
        // Experimental in 2.4.20: enables the explicit name-based destructuring syntax only
        // (destructuring/Destructuring.kt). Existing positional destructuring is unchanged.
        freeCompilerArgs.add("-Xname-based-destructuring=only-syntax")
    }
}
