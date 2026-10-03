package com.shelfwise.assistant.web

import com.shelfwise.assistant.ports.Answer
import com.shelfwise.assistant.ports.ProductAssistant
import com.shelfwise.assistant.ports.ProductHit
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

data class Question(val question: String)

// The controller sees only the port; which AI library answers is a profile decision.
@RestController
@RequestMapping("/api/assistant")
class AssistantController(private val assistant: ProductAssistant) {
    @GetMapping("/search")
    fun search(
        @RequestParam q: String,
        @RequestParam maxPricePaise: Long?,
        @RequestParam tag: String?,
    ): List<ProductHit> = assistant.search(q, maxPricePaise, tag)

    @PostMapping("/ask")
    fun ask(@RequestBody body: Question): Answer = assistant.ask(body.question)
}
