package org.binance.pastdataservice.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.model.dto.response.CandleDto;
import org.binance.pastdataservice.model.entity.Trade;
import org.binance.pastdataservice.repository.TradeRepository;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeService {
    private final TradeRepository tradeRepository;
    public String insert(CreateTradeDto createTradeDto) {
        return tradeRepository.save(createTradeDto.toEntity()).getId();
    }

    @CacheEvict(value = "candles", allEntries = true)
    @Transactional
    public void insertBatch(List<CreateTradeDto> dtos) {
        List<Trade> trades = dtos.stream()
                .map(CreateTradeDto::toEntity)
                .collect(Collectors.toList());
        tradeRepository.saveAll(trades);
//        log.info("Batch inserted {} trades", trades.size());
    }


    @Cacheable(value = "candles", key = "#symbol + '-' + #from.toString() + '-' + #to.toString() + '-' + #tickSize")
    public List<CandleDto> findBySymbolAndFilters(
            String symbol,
            LocalDateTime from,
            LocalDateTime to,
            String tickSize
    ) {
        long fromEpoch = from.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long toEpoch = to.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long tickSizeMs = parseTickSize(tickSize);
        
        List<Trade> trades = tradeRepository.findBySymbolAndTimeRange(symbol, fromEpoch, toEpoch);
        return aggregateToCandles(trades, tickSizeMs);
    }

    private List<CandleDto> aggregateToCandles(List<Trade> trades, long tickSizeMs) {
        if (trades.isEmpty()) {
            return Collections.emptyList();
        }

        // Filter out zero-price trades (defensive — may exist from before WebConsumer fix)
        List<Trade> validTrades = trades.stream()
                .filter(t -> t.getPrice() > 0 && t.getQuantity() > 0)
                .toList();

        if (validTrades.isEmpty()) {
            return Collections.emptyList();
        }

        // Grouping trades into time buckets
        Map<Long, List<Trade>> buckets = validTrades.stream()
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
                    double high = bucketTrades.stream().mapToDouble(Trade::getPrice).max().orElseThrow();
                    double low = bucketTrades.stream().mapToDouble(Trade::getPrice).min().orElseThrow();
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
