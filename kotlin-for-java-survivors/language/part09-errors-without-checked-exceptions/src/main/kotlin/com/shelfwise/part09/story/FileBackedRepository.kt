package com.shelfwise.part09.story

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

class FileBackedRepository(private val path: Path) {
    // Throws NoSuchFileException (an IOException) when the export is missing. Nothing in the signature says so.
    fun load(): List<String> = Files.readAllLines(path)

    // The same function, with the exception declared for Java's compiler.
    @Throws(IOException::class)
    fun loadChecked(): List<String> = Files.readAllLines(path)
}

fun main() {
    val repo = FileBackedRepository(Path.of("missing-export.csv"))
    try {
        repo.load()
    } catch (e: IOException) {
        println("Kotlin caught ${e::class.simpleName}") // -> Kotlin caught NoSuchFileException
    }
}
