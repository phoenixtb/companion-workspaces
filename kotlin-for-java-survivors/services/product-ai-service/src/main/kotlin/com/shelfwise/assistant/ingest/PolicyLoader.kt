package com.shelfwise.assistant.ingest

import com.shelfwise.assistant.ports.ProductIndexer
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.stereotype.Component

// Store policies are the knowledge the prototype lacked. They're indexed at startup, with
// deterministic ids, so restarting the service overwrites them instead of adding copies.
@Component
class PolicyLoader(private val indexer: ProductIndexer, private val resources: ResourcePatternResolver) : ApplicationRunner {
    override fun run(args: ApplicationArguments) {
        resources.getResources("classpath:policies/*.md").forEach { policy ->
            indexer.upsertPolicy(policy.filename!!.removeSuffix(".md"), policy.getContentAsString(Charsets.UTF_8))
        }
    }
}
