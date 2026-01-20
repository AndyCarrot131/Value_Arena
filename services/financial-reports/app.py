import schedule
import time
import logging
import os
from collector import FinancialReportCollector
from extractor import PDFTextExtractor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_job():
    """Collect new financial reports"""
    collector = FinancialReportCollector()
    try:
        collector.collect_all()
    except Exception as e:
        logger.error(f"Collection job failed: {e}", exc_info=True)
    finally:
        collector.close()


def extract_job():
    """Extract text from PDFs"""
    extractor = PDFTextExtractor()
    try:
        extractor.process_batch()
    except Exception as e:
        logger.error(f"Extraction job failed: {e}", exc_info=True)
    finally:
        extractor.close()

def summary_job():
    """AI Summary Job - 生成深度摘要"""
    logger.info("Starting Summary Job...")
    try:
        from summary import FinancialReportSummarizer
        summarizer = FinancialReportSummarizer()
        summarizer.process_batch()
        summarizer.close()
    except Exception as e:
        # 使用 exc_info=True 会打印完整的堆栈信息到 CloudWatch
        logger.error(f"Summary job encountered a fatal error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    logger.info("Financial reports service started")
    
    collect_job()
    time.sleep(30) 
    extract_job()  # 必须先跑完文本提取
    time.sleep(30) # 给数据库状态更新留一点时间
    summary_job()  # 最后跑总结

    # Schedule collection daily at 2:00 AM
    schedule.every().day.at("02:00").do(collect_job)
    
    # Schedule extraction every hour
    schedule.every().hour.do(extract_job)

    schedule.every(2).hours.do(summary_job)
    
    logger.info("Scheduled tasks configured: Collection(daily 02:00), Extraction(hourly)")
    
    while True:
        schedule.run_pending()
        time.sleep(60)