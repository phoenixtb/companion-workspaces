import java.io.File

// Convention for the Act I/II "language" modules: every example main() is run by
// the test kit and its stdout is compared with the `// ->` comments in the source.
plugins {
    id("kfs.kotlin-jvm")
}

dependencies {
    testImplementation(project(":language:testkit"))
}

tasks.withType<Test>().configureEach {
    // The test kit reads example sources to find the expected output.
    systemProperty("kfs.sourceRoots", listOf("src/main/kotlin", "src/main/java")
        .map { layout.projectDirectory.dir(it).asFile.absolutePath }
        .joinToString(File.pathSeparator))
}

// Prints the bytecode of one compiled class, as used in the "under the hood"
// sections: ./gradlew :language:part04-data-sealed-and-value:javap -Pclass=com.shelfwise.catalog.Price
tasks.register<Exec>("javap") {
    group = "help"
    description = "Disassembles -Pclass=<fully.qualified.Name> with javap -c -p."
    dependsOn("classes")
    val className = providers.gradleProperty("class")
    val classesDir = layout.buildDirectory.dir("classes/kotlin/main")
    val launcher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(25) }
    doFirst {
        val javap = launcher.get().metadata.installationPath.file("bin/javap").asFile
        val extra = providers.gradleProperty("javapFlags").getOrElse("-c -p").split(" ")
        commandLine(listOf(javap.absolutePath) + extra + listOf("-cp", classesDir.get().asFile.absolutePath, className.get()))
    }
}
