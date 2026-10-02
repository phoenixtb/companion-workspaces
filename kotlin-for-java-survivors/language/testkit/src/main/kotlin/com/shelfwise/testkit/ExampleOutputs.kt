package com.shelfwise.testkit

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DynamicTest
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.lang.reflect.InvocationTargetException

/**
 * Turns every example file with a `main` into a test.
 *
 * The test runs `main`, captures stdout, and compares it line by line with the
 * `// -> expected` comments in the source, in the order they appear. A `…` in an
 * expected line matches any text (for timings, random ids and hash codes).
 * Whole-line comments are ignored, so commented-out compile errors never count.
 *
 * Usage in a module:
 *
 * ```
 * class ExampleOutputTest {
 *     @TestFactory fun examples() = exampleOutputTests()
 * }
 * ```
 */
fun exampleOutputTests(): List<DynamicTest> =
    sourceRoots()
        .flatMap { root -> root.walkTopDown().filter { it.extension == "kt" || it.extension == "java" }.map { root to it } }
        .mapNotNull { (root, file) -> Example.parse(root, file) }
        .sortedBy { it.displayName }
        .map { example -> DynamicTest.dynamicTest(example.displayName) { example.verify() } }
        .also { assertTrue(it.isNotEmpty(), "No examples with main() found under ${sourceRoots()}") }

/** Runs [block] and returns everything it printed to stdout, one entry per line. */
fun captureStdout(block: () -> Unit): List<String> {
    val original = System.out
    val buffer = ByteArrayOutputStream()
    val capture = PrintStream(buffer, true, Charsets.UTF_8)
    System.setOut(capture)
    try {
        block()
    } finally {
        capture.flush()
        System.setOut(original)
    }
    return buffer.toString(Charsets.UTF_8).lines().map { it.trimEnd() }.dropLastWhile { it.isEmpty() }
}

private fun sourceRoots(): List<File> =
    System.getProperty("kfs.sourceRoots", "src/main/kotlin")
        .split(File.pathSeparator)
        .map(::File)
        .filter { it.isDirectory }

private val EXPECTED = Regex("""//\s*->\s?(.*)$""")
private val PACKAGE = Regex("""^\s*package\s+([\w.]+)""", RegexOption.MULTILINE)
private val JVM_NAME = Regex("""@file:JvmName\("(\w+)"\)""")
private val KOTLIN_MAIN = Regex("""^(suspend\s+)?fun\s+main\s*\(""", RegexOption.MULTILINE)
private val JAVA_MAIN = Regex("""static\s+void\s+main\s*\(""")

private class Example(val displayName: String, val className: String, val expected: List<String>) {

    fun verify() {
        val mainMethod = Class.forName(className).getMethod("main", Array<String>::class.java)
        val actual = captureStdout {
            try {
                mainMethod.invoke(null, emptyArray<String>())
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
        }
        assertEquals(expected.size, actual.size, "Line count differs.\nExpected:\n${expected.joinToString("\n")}\nActual:\n${actual.joinToString("\n")}")
        expected.zip(actual).forEachIndexed { index, (want, got) ->
            assertTrue(matches(want, got), "Line ${index + 1}: expected <$want> but was <$got>")
        }
    }

    private fun matches(want: String, got: String): Boolean =
        if ("…" !in want) want == got
        else want.split("…").joinToString(".*") { Regex.escape(it) }.toRegex().matches(got)

    companion object {
        fun parse(root: File, file: File): Example? {
            val source = file.readText()
            val isKotlin = file.extension == "kt"
            if (isKotlin && !KOTLIN_MAIN.containsMatchIn(source)) return null
            if (!isKotlin && !JAVA_MAIN.containsMatchIn(source)) return null
            val pkg = PACKAGE.find(source)?.groupValues?.get(1)
            val simpleName = when {
                !isKotlin -> file.nameWithoutExtension
                else -> JVM_NAME.find(source)?.groupValues?.get(1) ?: (file.nameWithoutExtension + "Kt")
            }
            val expected = source.lines()
                .filterNot { it.trimStart().startsWith("//") }
                .mapNotNull { line -> EXPECTED.find(line)?.groupValues?.get(1)?.trimEnd() }
            return Example(
                displayName = file.relativeTo(root).path,
                className = listOfNotNull(pkg, simpleName).joinToString("."),
                expected = expected,
            )
        }
    }
}
