package org.binance.pastdataservice;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataListener {

    @Transactional
    @RabbitListener(queues = "${rabbit-mq.consumer.queue}")
    public void storeData(String data) {
        try {
            log.info("Received data: {}", data);
        } catch (Exception e) {
            log.error("Error processing data: {}", data, e);
            throw new RuntimeException("Failed to process data: " + data, e);
        }
    }
}
