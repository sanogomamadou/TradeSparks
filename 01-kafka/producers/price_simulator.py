#01-kafka/producers/price_simulator.py
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration Kafka
import os
BROKER = os.environ.get("BROKER", "kafka:9092")

# Liste de 15 actifs (symboles/tickers), groupés par marché
ASSETS = {
    # Stocks (actions US)
    'stocks.AAPL': 187.10,
    'stocks.MSFT': 343.50,
    'stocks.GOOG': 139.64,
    'stocks.AMDB': 124.42,
    'stocks.TSLA': 233.96,
    'stocks.META': 324.82,
    'stocks.NVDA': 462.00,
    # Forex
    'forex.EURUSD': 1.0765,
    'forex.USDJPY': 148.68,
    'forex.GBPUSD': 1.2447,
    'forex.USDCAD': 1.3692,
    # Crypto
    'crypto.BTCUSD': 34960.00,
    'crypto.ETHUSD': 1890.00,
    'crypto.XRPUSD': 0.608,
    'crypto.SOLUSD': 42.70
}

# Initialisation du producteur Kafka
ATTEMPTS = 12  # 1 minute max

for attempt in range(ATTEMPTS):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except Exception as e:
        print(f"Kafka unreachable at {BROKER} (attempt {attempt+1}), waiting 5s...")
        time.sleep(5)
else:
    print("Error: Kafka broker not available after multiple retries.")
    sys.exit(1)


def simulate_price(base, volatility=0.015):
    # Simule une variation aléatoire typique de tick
    return round(base * (1 + random.uniform(-volatility, volatility)), 5 if base < 5 else 2)

def simulate_volume(symbol):
    # Volume simulé selon le type d’actif
    if symbol.startswith('stocks'):
        return random.randint(100, 5000)
    elif symbol.startswith('forex'):
        return random.randint(100000, 5000000)
    elif symbol.startswith('crypto'):
        return random.randint(1, 300)
    return 0

if __name__ == '__main__':
    print("Simulation Kafka ticks sur 15 actifs – TradeSparks")
    while True:
        for topic, base_price in ASSETS.items():
            tick = {
                'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                'symbol': topic.split('.')[-1],
                'market': topic.split('.')[0],
                'price': simulate_price(base_price),
                'volume': simulate_volume(topic)
            }
            producer.send(topic, value=tick)
            print(f"Envoyé vers {topic}: {tick}")
        producer.flush()
        time.sleep(0.5)  # ≈ 2 ticks/sec par actif
