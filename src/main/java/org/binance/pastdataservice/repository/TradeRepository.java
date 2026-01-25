package org.binance.pastdataservice.repository;

import org.binance.pastdataservice.model.entity.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TradeRepository extends JpaRepository<Trade, String> {

}
