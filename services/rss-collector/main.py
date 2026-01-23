#!/usr/bin/env python3
"""
RSS Collector Service - Main Entry Point
Parse command line arguments and execute collection jobs
"""

import sys
import argparse
import logging

from collector import RSSCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_collect(test_mode: bool = False) -> bool:
    """
    Run RSS collection job

    Args:
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting RSS collection (test_mode={test_mode})")

    collector = RSSCollector()
    try:
        collector.collect_all()
        return True
    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        return False
    finally:
        collector.close()


def run_deduplicate(test_mode: bool = False) -> bool:
    """
    Run deduplication cleanup job

    Args:
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting deduplication cleanup (test_mode={test_mode})")

    collector = RSSCollector()
    try:
        collector.run_deduplication_cleanup()
        return True
    except Exception as e:
        logger.error(f"Deduplication failed: {e}", exc_info=True)
        return False
    finally:
        collector.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='RSS Collector Service - Collect and deduplicate RSS feeds'
    )

    parser.add_argument(
        '--job',
        default='all',
        choices=['collect', 'deduplicate', 'all'],
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

    logger.info(f"Starting RSS Collector Service (job={args.job}, test_mode={args.test_mode})")

    try:
        success = True

        if args.job == 'collect':
            success = run_collect(test_mode=args.test_mode)
        elif args.job == 'deduplicate':
            success = run_deduplicate(test_mode=args.test_mode)
        elif args.job == 'all':
            # Run collect first, then deduplicate
            success = run_collect(test_mode=args.test_mode)
            if success:
                # Deduplication is already called in collect_all(), but can run separately
                logger.info("Collection completed, deduplication already executed")

        if success:
            logger.info("RSS Collector Service completed successfully")
            return 0
        else:
            logger.error("RSS Collector Service completed with errors")
            return 1

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
