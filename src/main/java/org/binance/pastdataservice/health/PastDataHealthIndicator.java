package org.binance.pastdataservice.health;

import lombok.RequiredArgsConstructor;
import org.binance.pastdataservice.DataListener;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PastDataHealthIndicator implements HealthIndicator {

    private final DataListener dataListener;

    @Override
    public Health health() {
        int bufferSize = dataListener.getBufferSize();
        long totalReceived = dataListener.getTotalTradesReceived().get();
        long totalBatches = dataListener.getTotalBatchesInserted().get();
        String lastBatch = dataListener.getLastBatchTime() != null
                ? dataListener.getLastBatchTime().toString()
                : "No batches";

        Health.Builder builder = totalReceived > 0 ? Health.up() : Health.unknown();

        return builder
                .withDetail("bufferSize", bufferSize)
                .withDetail("totalTradesReceived", totalReceived)
                .withDetail("totalBatchesInserted", totalBatches)
                .withDetail("lastBatchInsertTime", lastBatch)
                .build();
    }
}