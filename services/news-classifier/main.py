#!/usr/bin/env python3
"""
News Classifier Service - Main Entry Point
Parse command line arguments and execute classification jobs
"""

import sys
import argparse
import logging

from classifier import NewsClassifier

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_classify(batch_size: int = 100, test_mode: bool = False) -> bool:
    """
    Run news classification job

    Args:
        batch_size: Number of articles to classify in one batch
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting news classification (batch_size={batch_size}, test_mode={test_mode})")

    classifier = NewsClassifier()
    try:
        classifier.classify_batch()
        return True
    except Exception as e:
        logger.error(f"Classification failed: {e}", exc_info=True)
        return False
    finally:
        classifier.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='News Classifier Service - Classify news articles using FinBERT'
    )

    parser.add_argument(
        '--job',
        default='classify',
        choices=['classify'],
        help='Job to execute (default: classify)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of articles to classify per batch (default: 100)'
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

    logger.info(f"Starting News Classifier Service (job={args.job}, batch_size={args.batch_size}, test_mode={args.test_mode})")

    try:
        success = True

        if args.job == 'classify':
            success = run_classify(batch_size=args.batch_size, test_mode=args.test_mode)

        if success:
            logger.info("News Classifier Service completed successfully")
            return 0
        else:
            logger.error("News Classifier Service completed with errors")
            return 1

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
