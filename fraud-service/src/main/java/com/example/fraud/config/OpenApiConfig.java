package com.example.fraud.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI fraudServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("fraud-service")
                .version("v1")
                .description("Evaluación de fraude de las transferencias. Consume transfer.created, aplica las "
                        + "reglas SAME_ACCOUNT, AMOUNT_EXCEEDED y VELOCITY_EXCEEDED, y publica transfer.evaluated."));
    }
}
