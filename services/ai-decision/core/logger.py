"""
Structured Logger
Structured logging utilities with JSON output for CloudWatch
"""

import logging
import json
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

# US Eastern Time (ET) - Fixed offset UTC-04:00
ET_OFFSET = timezone(timedelta(hours=-4))


class JsonFormatter(logging.Formatter):
    """JSON formatter"""
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as JSON
        
        Args:
            record: log record
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            'timestamp': datetime.now(ET_OFFSET).isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name,
        }
        
        # Add extra fields
        if hasattr(record, 'agent_id'):
            log_data['agent_id'] = record.agent_id
        
        if hasattr(record, 'workflow'):
            log_data['workflow'] = record.workflow
        
        if hasattr(record, 'details'):
            log_data['details'] = record.details
        
        # Add exception info
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add file location (DEBUG only)
        if record.levelno == logging.DEBUG:
            log_data['file'] = f"{record.filename}:{record.lineno}"
            log_data['function'] = record.funcName
        
        return json.dumps(log_data, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Text formatter (for local development)"""
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


def setup_logger(
    name: str = 'ai-decision',
    level: str = 'INFO',
    log_format: str = 'json'
) -> logging.Logger:
    """
    Configure a logger
    
    Args:
        name: logger name
        level: log level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        log_format: log format (json/text)
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    
    # Set formatter
    if log_format.lower() == 'json':
        formatter = JsonFormatter()
    else:
        formatter = TextFormatter()
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


# Global logger instance
_logger_instance: Optional[logging.Logger] = None


def get_logger(
    name: str = 'ai-decision',
    level: str = 'INFO',
    log_format: str = 'json'
) -> logging.Logger:
    """
    Get the global logger
    
    Args:
        name: logger name
        level: log level
        log_format: log format
        
    Returns:
        Logger instance
    """
    global _logger_instance
    
    if _logger_instance is None:
        _logger_instance = setup_logger(name, level, log_format)
    
    return _logger_instance


class LoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that automatically adds context
    
    Example:
        logger = get_logger()
        context_logger = LoggerAdapter(logger, {'agent_id': 'claude', 'workflow': 'trading_decision'})
        context_logger.info('Generated decision', extra={'details': {'symbol': 'NVDA'}})
    """
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """
        Process the log message and add context
        
        Args:
            msg: log message
            kwargs: extra parameters
            
        Returns:
            Processed (msg, kwargs)
        """
        # Merge context and extra parameters
        extra = kwargs.get('extra', {})
        extra.update(self.extra)
        kwargs['extra'] = extra
        
        return msg, kwargs


def create_context_logger(
    agent_id: Optional[str] = None,
    workflow: Optional[str] = None,
    **kwargs
) -> LoggerAdapter:
    """
    Create a logger with context
    
    Args:
        agent_id: AI ID
        workflow: workflow name
        **kwargs: other context fields
    
    Returns:
        Logger adapter instance
        
    Example:
        logger = create_context_logger(agent_id='claude', workflow='trading_decision')
        logger.info('Starting workflow')
        logger.error('Failed to generate decision', extra={'details': {'error': 'timeout'}})
    """
    context = {}
    
    if agent_id:
        context['agent_id'] = agent_id
    
    if workflow:
        context['workflow'] = workflow
    
    context.update(kwargs)
    
    return LoggerAdapter(get_logger(), context)
