package com.shelfwise.story

import org.springframework.boot.autoconfigure.SpringBootApplication

// A test-only application for the opening's data-class entities, in its own package so they
// never reach the real service's entity scan or schema validation.
@SpringBootApplication
class StoryApplication
