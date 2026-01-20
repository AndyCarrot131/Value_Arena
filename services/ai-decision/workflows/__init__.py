"""
Workflows Module
Export all workflow classes
"""

from .hourly_news_analysis import HourlyNewsAnalysisWorkflow
from .daily_summary import DailySummaryWorkflow
from .trading_decision import TradingDecisionWorkflow
from .weekly_summary import WeeklySummaryWorkflow
from .stock_analysis import StockAnalysisWorkflow

__all__ = [
    'HourlyNewsAnalysisWorkflow',
    'DailySummaryWorkflow',
    'TradingDecisionWorkflow',
    'WeeklySummaryWorkflow',
    'StockAnalysisWorkflow',
]
