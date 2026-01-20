"""
Core Infrastructure Module
Core infrastructure module exporting all core classes
"""

from .secrets_manager import SecretsManager, get_secrets_manager
from .database import DatabaseManager, get_database_manager
from .redis_client import RedisClient, get_redis_client
from .ai_client import AIClient, get_ai_client
from .bedrock_client import BedrockClient, get_bedrock_client
from .opensearch_client import OpenSearchClient, get_opensearch_client
from .logger import setup_logger, get_logger, create_context_logger

__all__ = [
    # Secrets Manager
    'SecretsManager',
    'get_secrets_manager',
    
    # Database
    'DatabaseManager',
    'get_database_manager',
    
    # Redis
    'RedisClient',
    'get_redis_client',
    
    # AI Client
    'AIClient',
    'get_ai_client',
    
    # Bedrock Client
    'BedrockClient',
    'get_bedrock_client',
    
    # OpenSearch Client
    'OpenSearchClient',
    'get_opensearch_client',
    
    # Logger
    'setup_logger',
    'get_logger',
    'create_context_logger',
]
