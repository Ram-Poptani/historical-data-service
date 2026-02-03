package org.binance.pastdataservice.controllers;


import lombok.RequiredArgsConstructor;
import org.binance.pastdataservice.model.dto.response.CandleDto;
import org.binance.pastdataservice.service.TradeService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import jakarta.validation.constraints.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/trades")
@RequiredArgsConstructor
@Validated
public class TradeController {

    private final TradeService tradeService;

    @GetMapping("/{symbol}")
    public List<CandleDto> getCandles(
            @PathVariable @NotBlank @Size(min = 2, max = 20) String symbol,

            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime from,

            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime to,

            @RequestParam(defaultValue = "1m")
            @Pattern(regexp = "^(1ms|1s|1m|5m|15m|1h|4h|1d)$", message = "Invalid tick size")
            String tickSize
    ) {
        LocalDateTime now = LocalDateTime.now();
        if (to == null) {
            to = now;
        }
        if (from == null) {
            from = now.minusDays(60);
        }
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("'from' must be before 'to'");
        }
        return tradeService.findBySymbolAndFilters(symbol, from, to, tickSize);
    }


}


