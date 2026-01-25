package org.binance.pastdataservice.service;

import lombok.RequiredArgsConstructor;
import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.repository.TradeRepository;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TradeService {
    private final TradeRepository tradeRepository;
    public String insert(CreateTradeDto createTradeDto) {
        return tradeRepository.save(createTradeDto.toEntity()).getId();
    }
}
