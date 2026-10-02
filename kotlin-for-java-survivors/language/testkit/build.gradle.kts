plugins {
    id("kfs.kotlin-jvm")
}

dependencies {
    api(platform(libs.junit.bom))
    api(libs.junit.jupiter.api)
}
