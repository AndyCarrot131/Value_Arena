#!/usr/bin/env python3
"""
Financial Reports Service - Main Entry Point
Parse command line arguments and execute financial report jobs
"""

import sys
import argparse
import logging
import time

from collector import FinancialReportCollector
from extractor import PDFTextExtractor
from summary import FinancialReportSummarizer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_collect(test_mode: bool = False) -> bool:
    """
    Run financial report collection job

    Args:
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting financial report collection (test_mode={test_mode})")

    collector = FinancialReportCollector()
    try:
        collector.collect_all()
        return True
    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        return False
    finally:
        collector.close()


def run_extract(test_mode: bool = False) -> bool:
    """
    Run PDF text extraction job

    Args:
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting text extraction (test_mode={test_mode})")

    extractor = PDFTextExtractor()
    try:
        extractor.process_batch()
        return True
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return False
    finally:
        extractor.close()


def run_summary(test_mode: bool = False) -> bool:
    """
    Run AI summary generation job

    Args:
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting AI summary generation (test_mode={test_mode})")

    try:
        summarizer = FinancialReportSummarizer()
        summarizer.process_batch()
        summarizer.close()
        return True
    except Exception as e:
        logger.error(f"Summary generation failed: {e}", exc_info=True)
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Financial Reports Service - Collect, extract, and summarize financial reports'
    )

    parser.add_argument(
        '--job',
        default='all',
        choices=['collect', 'extract', 'summary', 'all'],
        help='Job to execute (default: all)'
    )

    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level'
    )

    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run in test mode (no database writes)'
    )

    args = parser.parse_args()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info(f"Starting Financial Reports Service (job={args.job}, test_mode={args.test_mode})")

    try:
        success = True

        if args.job == 'collect':
            success = run_collect(test_mode=args.test_mode)
        elif args.job == 'extract':
            success = run_extract(test_mode=args.test_mode)
        elif args.job == 'summary':
            success = run_summary(test_mode=args.test_mode)
        elif args.job == 'all':
            # Run all jobs sequentially
            success = run_collect(test_mode=args.test_mode)
            if success:
                logger.info("Collection completed, waiting 30s before extraction...")
                time.sleep(30)
                success = run_extract(test_mode=args.test_mode)
            if success:
                logger.info("Extraction completed, waiting 30s before summary...")
                time.sleep(30)
                success = run_summary(test_mode=args.test_mode)

        if success:
            logger.info("Financial Reports Service completed successfully")
            return 0
        else:
            logger.error("Financial Reports Service completed with errors")
            return 1

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
