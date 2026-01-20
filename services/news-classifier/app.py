import schedule
import time
import logging
import os
from classifier import NewsClassifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def job():
    """Scheduled classification task"""
    classifier = NewsClassifier()
    try:
        classifier.classify_batch()
    except Exception as e:
        logger.error(f"Classification task failed: {e}", exc_info=True)
    finally:
        classifier.close()


if __name__ == "__main__":
    logger.info("News Classifier Service starting...")

    # Execute immediately once
    job()

    # Execute every N minutes (after RSS collection)
    interval_minutes = int(os.getenv('CLASSIFY_INTERVAL_MINUTES', '10'))
    logger.info(f"Task scheduled: every {interval_minutes} minutes")
    
    schedule.every(interval_minutes).minutes.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(60)