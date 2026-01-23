"""
Configuration Management
Load configuration from AWS Secrets Manager or environment variables
"""

import os
import json
from typing import Optional


class Settings:
    """Application configuration manager"""
    
    def __init__(self, dev_mode: bool = False):
        """
        Initialize settings
        
        Args:
            dev_mode: If True, use environment variables; if False, use Secrets Manager
        """
        self.dev_mode = dev_mode
        self._db_config: Optional[dict] = None
        self._opensearch_config: Optional[dict] = None
        
        # Load configuration on initialization if in dev mode
        if dev_mode:
            self._load_from_env()
    
    def _load_from_env(self):
        """Load configuration from environment variables (dev mode)"""
        # AWS Region
        self._region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Database
        self._db_host = os.getenv('DB_HOST')
        self._db_port = int(os.getenv('DB_PORT', '5432'))
        self._db_name = os.getenv('DB_NAME')
        self._db_user = os.getenv('DB_USER')
        self._db_password = os.getenv('DB_PASSWORD')
        
        # Redis
        self._redis_host = os.getenv('REDIS_HOST')
        self._redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self._redis_ssl = os.getenv('REDIS_SSL', 'true').lower() == 'true'
        
        # BaiCai API
        self._baicai_api_url = os.getenv('BAICAI_API_URL')
        self._baicai_api_key = os.getenv('BAICAI_API_KEY')
        
        # OpenSearch
        self._opensearch_endpoint = os.getenv('OPENSEARCH_ENDPOINT')
        self._opensearch_service = os.getenv('OPENSEARCH_SERVICE', 'es')  # 'es' for Provisioned, 'aoss' for Serverless
        self._index_name = os.getenv('INDEX_NAME', 'ai-investment-decisions')
        self._knowledge_base_id = os.getenv('KNOWLEDGE_BASE_ID')
    
    def _load_from_secrets_manager(self):
        """Load configuration from AWS Secrets Manager (production mode)"""
        import boto3
        
        secrets_client = boto3.client('secretsmanager', region_name=self.region)
        
        # Load database config
        try:
            response = secrets_client.get_secret_value(
                SecretId=os.getenv('SECRET_DATABASE_CONFIG', 'ai-stock-war/database-config')
            )
            self._db_config = json.loads(response['SecretString'])
        except Exception as e:
            raise RuntimeError(f"Failed to load database config: {e}")
        
        # Load OpenSearch config
        try:
            response = secrets_client.get_secret_value(
                SecretId=os.getenv('SECRET_OPENSEARCH_CONFIG', 'ai-stock-war/opensearch-config')
            )
            self._opensearch_config = json.loads(response['SecretString'])
        except Exception as e:
            raise RuntimeError(f"Failed to load OpenSearch config: {e}")
    
    @property
    def region(self) -> str:
        """AWS Region"""
        if self.dev_mode:
            return self._region
        return os.getenv('AWS_REGION', 'us-east-1')
    
    @property
    def db_host(self) -> str:
        """Database host"""
        if self.dev_mode:
            return self._db_host
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['db_host']
    
    @property
    def db_port(self) -> int:
        """Database port"""
        if self.dev_mode:
            return self._db_port
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return int(self._db_config['db_port'])
    
    @property
    def db_name(self) -> str:
        """Database name"""
        if self.dev_mode:
            return self._db_name
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['db_name']
    
    @property
    def db_user(self) -> str:
        """Database user"""
        if self.dev_mode:
            return self._db_user
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['db_user']
    
    @property
    def db_password(self) -> str:
        """Database password"""
        if self.dev_mode:
            return self._db_password
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['db_password']
    
    @property
    def redis_host(self) -> str:
        """Redis host"""
        if self.dev_mode:
            return self._redis_host
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['redis_host']
    
    @property
    def redis_port(self) -> int:
        """Redis port"""
        if self.dev_mode:
            return self._redis_port
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return int(self._db_config['redis_port'])
    
    @property
    def redis_ssl(self) -> bool:
        """Redis SSL enabled"""
        if self.dev_mode:
            return self._redis_ssl
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config.get('redis_ssl', True)
    
    @property
    def baicai_api_url(self) -> str:
        """BaiCai API URL"""
        if self.dev_mode:
            return self._baicai_api_url
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['BAICAI_API_URL']
    
    @property
    def baicai_api_key(self) -> str:
        """BaiCai API Key"""
        if self.dev_mode:
            return self._baicai_api_key
        
        if not self._db_config:
            self._load_from_secrets_manager()
        return self._db_config['BAICAI_API_KEY']
    
    @property
    def opensearch_endpoint(self) -> str:
        """OpenSearch endpoint"""
        if self.dev_mode:
            return self._opensearch_endpoint
        
        if not self._opensearch_config:
            self._load_from_secrets_manager()
        return self._opensearch_config['collection_endpoint']
    
    @property
    def index_name(self) -> str:
        """OpenSearch index name"""
        if self.dev_mode:
            return self._index_name
        
        if not self._opensearch_config:
            self._load_from_secrets_manager()
        return self._opensearch_config['index_name']
    
    @property
    def opensearch_service(self) -> str:
        """OpenSearch service name ('es' for Provisioned, 'aoss' for Serverless)"""
        if self.dev_mode:
            return self._opensearch_service

        if not self._opensearch_config:
            self._load_from_secrets_manager()
        return self._opensearch_config.get('service', 'es')

    @property
    def knowledge_base_id(self) -> str:
        """Bedrock Knowledge Base ID"""
        if self.dev_mode:
            return self._knowledge_base_id

        if not self._opensearch_config:
            self._load_from_secrets_manager()
        return self._opensearch_config['knowledge_base_id']

    def get_api_key(self, api_key_env: str) -> str:
        """
        动态获取 API Key（支持多个 Key）

        Args:
            api_key_env: API Key 环境变量名（如 BAICAI_API_KEY, GEMINI_KEY）

        Returns:
            API Key 字符串
        """
        if self.dev_mode:
            # 开发模式：从环境变量获取
            key = os.getenv(api_key_env)
            if not key:
                raise ValueError(f"API Key not found in environment: {api_key_env}")
            return key

        # 生产模式：从 Secrets Manager 获取
        if not self._db_config:
            self._load_from_secrets_manager()

        if api_key_env not in self._db_config:
            raise ValueError(f"API Key not found in Secrets Manager: {api_key_env}")

        return self._db_config[api_key_env]


# Global settings instance
_settings: Optional[Settings] = None


def get_settings(dev_mode: bool = False) -> Settings:
    """
    Get global settings instance
    
    Args:
        dev_mode: If True, load from environment variables
        
    Returns:
        Settings instance
    """
    global _settings
    
    if _settings is None:
        # Check if running in dev mode from environment
        env_dev_mode = os.getenv('DEV_MODE', 'false').lower() == 'true'
        _settings = Settings(dev_mode=dev_mode or env_dev_mode)
    
    return _settings


def reload_settings(dev_mode: bool = False):
    """
    Reload settings (force refresh)
    
    Args:
        dev_mode: If True, load from environment variables
    """
    global _settings
    _settings = Settings(dev_mode=dev_mode)