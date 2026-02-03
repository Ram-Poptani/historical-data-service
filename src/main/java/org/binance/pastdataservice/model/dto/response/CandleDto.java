package org.binance.pastdataservice.model.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CandleDto {
    private long openTime;
    private long closeTime;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;
    private int tradeCount;
}
