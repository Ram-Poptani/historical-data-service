package org.binance.pastdataservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.model.dto.response.CandleDto;
import org.binance.pastdataservice.model.entity.Trade;
import org.binance.pastdataservice.repository.TradeRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
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


    public List<CandleDto> findBySymbolAndFilters(
            String symbol,
            LocalDateTime from,
            LocalDateTime to,
            String tickSize
    ) {
        long fromEpoch = from.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long toEpoch = to.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long tickSizeMs = parseTickSize(tickSize);
        
        List<Trade> trades = tradeRepository.findBySymbolAndTimeRange(symbol, fromEpoch, toEpoch);
        return aggregateToCandles(trades, tickSizeMs);
    }

    private List<CandleDto> aggregateToCandles(List<Trade> trades, long tickSizeMs) {
        if (trades.isEmpty()) {
            return Collections.emptyList();
        }

        // Groupping trades
        Map<Long, List<Trade>> buckets = trades.stream()
                .collect(Collectors.groupingBy(
                        trade -> (trade.getTradeTime() / tickSizeMs) * tickSizeMs,
                        TreeMap::new,
                        Collectors.toList()
                ));

        // Convert each bucket to a candle
        return buckets.entrySet().stream()
                .map(entry -> {
                    long bucketTime = entry.getKey();
                    List<Trade> bucketTrades = entry.getValue();
                    
                    bucketTrades.sort(Comparator.comparingLong(Trade::getTradeTime));
                    
                    double open = bucketTrades.get(0).getPrice();
                    double close = bucketTrades.get(bucketTrades.size() - 1).getPrice();
                    double high = bucketTrades.stream().mapToDouble(Trade::getPrice).max().orElse(0);
                    double low = bucketTrades.stream().mapToDouble(Trade::getPrice).min().orElse(0);
                    double volume = bucketTrades.stream().mapToDouble(Trade::getQuantity).sum();
                    
                    return CandleDto.builder()
                            .openTime(bucketTime)
                            .closeTime(bucketTime + tickSizeMs - 1)
                            .open(open)
                            .high(high)
                            .low(low)
                            .close(close)
                            .volume(volume)
                            .tradeCount(bucketTrades.size())
                            .build();
                })
                .collect(Collectors.toList());
    }

    private long parseTickSize(String tickSize) {
        return switch (tickSize) {
            case "1ms" -> 1L;
            case "1s" -> 1000L;
            case "1m" -> 60 * 1000L;
            case "5m" -> 5 * 60 * 1000L;
            case "15m" -> 15 * 60 * 1000L;
            case "1h" -> 60 * 60 * 1000L;
            case "4h" -> 4 * 60 * 60 * 1000L;
            case "1d" -> 24 * 60 * 60 * 1000L;
            default -> 60 * 1000L; // default 1m
        };
    }

}
