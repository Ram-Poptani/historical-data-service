package org.binance.pastdataservice.model.dto.request;

import org.binance.pastdataservice.model.entity.Trade;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateTradeDto {
//    {"e":"trade","E":1769333690922,"T":1769333690922,"s":"XRPUSDT","t":2939595425,"p":"1.8933","q":"147.7","X":"MARKET","m":false}
    private String s;      // symbol
    private long t;        // tradeId
    private String p;      // price
    private String q;      // quantity

    @JsonProperty("X")
    private String tradeType;

    private boolean m;     // isBuyerMaker

    @JsonProperty("E")
    private long eventTime;

    @JsonProperty("T")
    private long tradeTime;

    public Trade toEntity() {
        return new Trade(
            null,
            this.s,
            this.t,
            Double.parseDouble(this.p),
            Double.parseDouble(this.q),
            this.tradeType,
            this.m,
            this.eventTime,
            this.tradeTime
        );
    }
}
