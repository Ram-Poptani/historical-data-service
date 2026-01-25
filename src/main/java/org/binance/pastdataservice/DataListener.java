package org.binance.pastdataservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.binance.pastdataservice.model.dto.request.CreateTradeDto;
import org.binance.pastdataservice.service.TradeService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataListener {

    private final JsonMapper jsonMapper;
    private final ObjectMapper objectMapper;
    private final TradeService tradeService;

    @Transactional
    @RabbitListener(queues = "${rabbit-mq.consumer.queue}")
    public void storeData(String data) {
        try {
            JsonNode jsonNode = jsonMapper.readTree(data);
            CreateTradeDto dto = objectMapper.treeToValue(jsonNode, CreateTradeDto.class);
            this.tradeService.insert(dto);
        } catch (Exception e) {
            log.error("Error processing data: {}", data, e);
            throw new RuntimeException("Failed to process data: " + data, e);
        }
    }
}
