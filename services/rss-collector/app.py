import schedule
import time
import logging
import os
from collector import RSSCollector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def job():
    """定时执行的任务"""
    collector = RSSCollector()
    try:
        collector.collect_all()
    except Exception as e:
        logger.error(f"collector failed: {e}", exc_info=True)
    finally:
        collector.close()


if __name__ == "__main__":
    logger.info("RSS collector service started.")
    logger.info("Using database-only deduplication (URL + Title Similarity)")
    logger.info("job will run every 2 hrs")
    
    # 立即执行一次
    job()
    
    # 每120分钟执行一次
    interval_minutes = int(os.getenv('FETCH_INTERVAL_MINUTES', '120'))
    schedule.every(interval_minutes).minutes.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(600)  # 每10分钟检查一次