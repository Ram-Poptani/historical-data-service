package org.binance.pastdataservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.model.entity.Trade;
import org.binance.pastdataservice.repository.TradeRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeService {
    private final TradeRepository tradeRepository;
    public String insert(CreateTradeDto createTradeDto) {
        return tradeRepository.save(createTradeDto.toEntity()).getId();
    }

    @Transactional
    public void insertBatch(List<CreateTradeDto> dtos) {
        List<Trade> trades = dtos.stream()
                .map(CreateTradeDto::toEntity)
                .collect(Collectors.toList());
        tradeRepository.saveAll(trades);
        log.info("Batch inserted {} trades", trades.size());
    }
}
