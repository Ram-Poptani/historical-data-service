package org.binance.pastdataservice.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.model.dto.response.CandleDto;
import org.binance.pastdataservice.model.entity.Trade;
import org.binance.pastdataservice.repository.TradeRepository;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeService {
    private final TradeRepository tradeRepository;
    private final CacheManager cacheManager;

    public String insert(CreateTradeDto createTradeDto) {
        return tradeRepository.save(createTradeDto.toEntity()).getId();
    }

    @Transactional
    public void insertBatch(List<CreateTradeDto> dtos) {
        List<Trade> trades = dtos.stream()
                .map(CreateTradeDto::toEntity)
                .collect(Collectors.toList());
        tradeRepository.saveAll(trades);
//        log.info("Batch inserted {} trades", trades.size());
    }


    public List<CandleDto> findBySymbolAndFilters(
            String symbol,
            LocalDateTime from,
            LocalDateTime to,
            String tickSize
    ) {

        long fromEpochRounded = from.truncatedTo(ChronoUnit.HOURS).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long toEpochRounded = to.truncatedTo(ChronoUnit.HOURS).plusHours(1).minusNanos(1).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long tickSizeMs = parseTickSize(tickSize);

        String cacheKey = symbol + "-" + from.toString() + "-" + to.toString() + "-" + tickSize;

        Cache cache = cacheManager.getCache("candles");
        if (cache != null) {
            Cache.ValueWrapper cachedValue = cache.get(cacheKey);
            if (cachedValue != null) {
                log.info("Cache hit for key: {}", cacheKey);
                return (List<CandleDto>) cachedValue.get();
            } else {
                log.info("Cache miss for key: {}", cacheKey);
            }
        }



        List<CandleDto> result =  tradeRepository.findCandlesBySymbolAndTimeRange(symbol, fromEpochRounded, toEpochRounded, tickSizeMs)
                .stream()
                .map(p -> CandleDto.builder()
                        .openTime(p.getOpenTime())
                        .closeTime(p.getOpenTime() + tickSizeMs - 1)
                        .open(p.getOpen())
                        .high(p.getHigh())
                        .low(p.getLow())
                        .close(p.getClose())
                        .volume(p.getVolume())
                        .tradeCount((int) p.getTradeCount())
                        .build())
                .collect(Collectors.toList());
        cache.put(cacheKey, result);
        return result;
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
