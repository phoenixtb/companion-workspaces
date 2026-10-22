package com.dispatchly.tracking;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

@Configuration
class ChangeEventJson {

    // A mapper just for Debezium's JSON: snake_case column names, and unknown fields ignored, so a
    // column added to shipments doesn't break the projection before the code knows about it.
    @Bean
    JsonMapper changeEventMapper() {
        return JsonMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }
}
