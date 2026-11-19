package com.example.fraud.api;

import com.example.fraud.service.EvaluationNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Respuestas de error como ProblemDetail (RFC 9457). La clase base ya cubre los errores estándar de Spring MVC
 * (por ejemplo, un transferId que no es UUID) con 400.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(EvaluationNotFoundException.class)
    public ProblemDetail handleNotFound(EvaluationNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Evaluation not found");
        return problem;
    }
}
