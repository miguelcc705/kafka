package com.example.transfer.api;

import com.example.transfer.domain.Transfer;
import com.example.transfer.service.TransferService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/transfers")
public class TransferController {

    private final TransferService transferService;

    public TransferController(TransferService transferService) {
        this.transferService = transferService;
    }

    @Operation(summary = "Crea una transferencia en estado PENDING; el fraude se evalúa de forma asíncrona")
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CreateTransferResponse create(@Valid @RequestBody CreateTransferRequest request) {
        Transfer transfer = transferService.create(request);
        return new CreateTransferResponse(transfer.getId(), transfer.getStatus());
    }

    @Operation(summary = "Consulta una transferencia por id")
    @GetMapping("/{id}")
    public TransferResponse get(@PathVariable UUID id) {
        return TransferResponse.from(transferService.get(id));
    }

    @Operation(summary = "Lista las transferencias de una cuenta origen, de la más reciente a la más antigua")
    @GetMapping
    public PageResponse<TransferResponse> list(@RequestParam @NotBlank String fromAccount,
                                               @RequestParam(defaultValue = "0") @Min(0) int page,
                                               @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return PageResponse.from(transferService.listByFromAccount(fromAccount, page, size), TransferResponse::from);
    }
}
