package com.example.transfer.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI transferServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("transfer-service")
                .version("v1")
                .description("Transferencias entre cuentas con evaluación de fraude asíncrona. "
                        + "Las transferencias se crean en PENDING y pasan a COMPLETED o REJECTED cuando "
                        + "fraud-service publica su decisión."));
    }
}
