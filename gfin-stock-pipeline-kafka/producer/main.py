import asyncio, aiohttp, yaml
from common.logger import get_logger
from producer.scraper import scrape_stock
from producer.kafka_producer import StockProducer

log = get_logger("producer-main")

# Load configuration from YAML
cfg = yaml.safe_load(open("config/app.yaml"))

async def run_once():
    """Run one scrape/produce cycle."""
    producer = StockProducer(
        cfg["kafka"]["bootstrap_servers"],
        cfg["kafka"]["topic"]
    )

    async with aiohttp.ClientSession() as session:
        for ticker in cfg["stocks"]["tickers"]:
            data = await scrape_stock(
                session,
                ticker,
                cfg["stocks"]["exchange"]
            )
            producer.send(data)

async def run_forever():
    """Run cycles forever with sleep in between."""
    while True:
        await run_once()
        log.info("All stock data produced, sleeping for next cycle...")
        await asyncio.sleep(cfg["runtime"]["scrape_interval_sec"])

def main():
    asyncio.run(run_forever())

if __name__ == "__main__":
    main()