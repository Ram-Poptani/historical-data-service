package org.binance.pastdataservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class DataListener {

    private static final int BATCH_SIZE = 100;

    private final JsonMapper jsonMapper;
    private final ObjectMapper objectMapper;
    private final TradeService tradeService;
    private final ConcurrentLinkedQueue<CreateTradeDto> buffer = new ConcurrentLinkedQueue<>();

    private final Counter tradesReceivedCounter;
    private final Timer batchInsertTimer;

    // Health tracking fields
    @Getter
    private volatile Instant lastBatchTime;
    @Getter
    private final AtomicLong totalTradesReceived = new AtomicLong(0);
    @Getter
    private final AtomicLong totalBatchesInserted = new AtomicLong(0);


    public DataListener(
            JsonMapper jsonMapper,
            ObjectMapper objectMapper,
            TradeService tradeService,
            MeterRegistry meterRegistry
    ) {
        this.jsonMapper = jsonMapper;
        this.objectMapper = objectMapper;
        this.tradeService = tradeService;
        this.tradesReceivedCounter = Counter.builder("pastdata.trades.received")
                .description("Total trades received from RabbitMQ")
                .register(meterRegistry);
        this.batchInsertTimer = Timer.builder("pastdata.batch.insert")
                .description("Time taken for batch DB inserts")
                .register(meterRegistry);
    }


    @Transactional
    @RabbitListener(queues = "${rabbit-mq.consumer.queue}")
    public void storeData(String data) {
        try {
            JsonNode jsonNode = jsonMapper.readTree(data);
            CreateTradeDto dto = objectMapper.treeToValue(jsonNode, CreateTradeDto.class);

            buffer.add(dto);
            totalTradesReceived.incrementAndGet();
            tradesReceivedCounter.increment();

            if (buffer.size() >= BATCH_SIZE) {
//                log.info("Buffer reached 100 items, processing batch...");
                this.flushBuffer();
            }

        } catch (Exception e) {
            log.error("Error processing data: {}", data, e);
            throw new RuntimeException("Failed to process data: " + data, e);
        }
    }

    private void flushBuffer() {
        List<CreateTradeDto> batch = new ArrayList<>(BATCH_SIZE);
        CreateTradeDto item;
        while ((item = buffer.poll()) != null) {
            batch.add(item);
        }
        if (batch.isEmpty()) return;

        batchInsertTimer.record(() -> tradeService.insertBatch(batch));
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