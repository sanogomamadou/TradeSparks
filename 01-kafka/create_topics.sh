#!/bin/bash
# Attente du démarrage complet de Kafka
sleep 10
for t in stocks.AAPL stocks.MSFT stocks.GOOG stocks.AMDB stocks.TSLA stocks.META stocks.NVDA \
         forex.EURUSD forex.USDJPY forex.GBPUSD forex.USDCAD \
         crypto.BTCUSD crypto.ETHUSD crypto.XRPUSD crypto.SOLUSD
do
  /usr/bin/kafka-topics --create --topic $t --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists
done
