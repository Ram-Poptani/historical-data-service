package org.binance.pastdataservice.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "trades", indexes = {
    @Index(name = "idx_symbol", columnList = "symbol")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Trade {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(nullable = false)
    private String symbol;

    @Column(nullable = false)
    private long tradeId;

    @Column(nullable = false)
    private double price;

    @Column(nullable = false)
    private double quantity;

    @Column(nullable = false)
    private String tradeType;

    @Column(nullable = false)
    private boolean isBuyerMaker;

    @Column(nullable = false)
    private long eventTime;

    @Column(nullable = false)
    private long tradeTime;

    public static boolean isPriceGt0(Trade trade) {
        return trade.getPrice() > 0;
    }

    public static boolean isQuantityNot0(Trade trade) {
        return trade.getQuantity() != 0;
    }
}
