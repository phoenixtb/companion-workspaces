package com.shelfwise.assistant

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class AssistantApplication

fun main(args: Array<String>) {
    runApplication<AssistantApplication>(*args)
}
