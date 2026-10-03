package com.shelfwise.part06.context

class Log(val name: String)

context(log: Log)
fun whoLogs(): String = log.name

fun main() {
    // Context arguments resolve by type. An inner context of the same type silently wins.
    context(Log("request")) {
        println(whoLogs()) // -> request
        context(Log("batch")) {
            println(whoLogs()) // -> batch
        }
    }
    // Two of the same type at the same level is ambiguous, like two candidate beans for @Autowired.
    // context(Log("a"), Log("b")) { whoLogs() }  // error: multiple potential context arguments for 'log: Log' in scope.
}
