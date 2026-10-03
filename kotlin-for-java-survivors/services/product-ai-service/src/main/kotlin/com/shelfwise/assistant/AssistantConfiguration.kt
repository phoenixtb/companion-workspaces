package com.shelfwise.assistant

import com.shelfwise.assistant.tools.CatalogClient
import org.springframework.context.annotation.Configuration
import org.springframework.web.service.registry.ImportHttpServices

@Configuration(proxyBeanMethods = false)
@ImportHttpServices(group = "catalog", types = [CatalogClient::class])
class AssistantConfiguration
