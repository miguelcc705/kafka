package com.example.fraud.api;

import com.example.fraud.service.EvaluationService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/evaluations")
public class EvaluationController {

    private final EvaluationService evaluationService;

    public EvaluationController(EvaluationService evaluationService) {
        this.evaluationService = evaluationService;
    }

    @Operation(summary = "Consulta la evaluación de fraude de una transferencia")
    @GetMapping("/{transferId}")
    public EvaluationResponse get(@PathVariable UUID transferId) {
        return EvaluationResponse.from(evaluationService.getByTransferId(transferId));
    }
}
