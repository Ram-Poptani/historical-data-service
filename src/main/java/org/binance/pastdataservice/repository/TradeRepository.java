package org.binance.pastdataservice.repository;

import org.binance.pastdataservice.model.entity.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TradeRepository extends JpaRepository<Trade, String> {


    @Query("SELECT t FROM Trade t WHERE t.symbol = :symbol AND t.tradeTime >= :from AND t.tradeTime <= :to ORDER BY t.tradeTime ASC")
    List<Trade> findBySymbolAndTimeRange(@Param("symbol") String symbol,
                                         @Param("from") long from,
                                         @Param("to") long to);
}
