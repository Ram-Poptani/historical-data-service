package org.binance.pastdataservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.service.TradeService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataListener {

    private static final int BATCH_SIZE = 100;

    private final JsonMapper jsonMapper;
    private final ObjectMapper objectMapper;
    private final TradeService tradeService;
    private final ConcurrentLinkedQueue<CreateTradeDto> buffer = new ConcurrentLinkedQueue<>();

    // Health tracking fields
    @Getter
    private volatile Instant lastBatchTime;
    @Getter
    private final AtomicLong totalTradesReceived = new AtomicLong(0);
    @Getter
    private final AtomicLong totalBatchesInserted = new AtomicLong(0);

    @Transactional
    @RabbitListener(queues = "${rabbit-mq.consumer.queue}")
    public void storeData(String data) {
        try {
            JsonNode jsonNode = jsonMapper.readTree(data);
            CreateTradeDto dto = objectMapper.treeToValue(jsonNode, CreateTradeDto.class);

            buffer.add(dto);
            totalTradesReceived.incrementAndGet();

            if (buffer.size() >= BATCH_SIZE) {
//                log.info("Buffer reached 100 items, processing batch...");
                this.flushBuffer();
            }

        } catch (Exception e) {
            log.error("Error processing data: {}", data, e);
            throw new RuntimeException("Failed to process data: " + data, e);
        }
    }

    private synchronized void flushBuffer() {
        if (buffer.isEmpty()) return;
        tradeService.insertBatch(buffer.stream().toList());
        buffer.clear();
        lastBatchTime = Instant.now();
        totalBatchesInserted.incrementAndGet();
    }

    public int getBufferSize() {
        return buffer.size();
    }

    @PreDestroy
    public void onShutdown() {
        log.info("Application is shutting down, flushing remaining {} data...", buffer.size());
        this.flushBuffer();
    }
}