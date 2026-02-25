package org.binance.pastdataservice.repository;

import java.util.List;

import org.binance.pastdataservice.model.entity.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface TradeRepository extends JpaRepository<Trade, String> {

    @Query("SELECT t FROM Trade t WHERE t.symbol = :symbol AND t.tradeTime >= :from AND t.tradeTime <= :to ORDER BY t.tradeTime ASC")
    List<Trade> findBySymbolAndTimeRange(@Param("symbol") String symbol,
                                         @Param("from") long from,
                                         @Param("to") long to);

    @Query(nativeQuery = true, value = """
            SELECT
                (trade_time / :tickMs) * :tickMs AS open_time,
                (ARRAY_AGG(price ORDER BY trade_time ASC))[1] AS open,
                MAX(price) AS high,
                MIN(price) AS low,
                (ARRAY_AGG(price ORDER BY trade_time DESC))[1] AS close,
                SUM(quantity) AS volume,
                COUNT(*) AS trade_count
            FROM trades
            WHERE symbol = :symbol
              AND trade_time >= :fromEpoch
              AND trade_time <= :toEpoch
              AND price > 0
              AND quantity != 0
            GROUP BY 1
            ORDER BY 1 ASC
            """)
    List<CandleProjection> findCandlesBySymbolAndTimeRange(
            @Param("symbol") String symbol,
            @Param("fromEpoch") long fromEpoch,
            @Param("toEpoch") long toEpoch,
            @Param("tickMs") long tickMs);

    interface CandleProjection {
        long getOpenTime();
        double getOpen();
        double getHigh();
        double getLow();
        double getClose();
        double getVolume();
        long getTradeCount();
    }
}
