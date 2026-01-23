"""
Configuration Management for RSS Collector
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
        self._config: Optional[dict] = None

        if dev_mode:
            self._load_from_env()

    def _load_from_env(self):
        """Load configuration from environment variables (dev mode)"""
        self._region = os.getenv('AWS_REGION', 'us-east-1')
        self._db_host = os.getenv('DB_HOST')
        self._db_port = int(os.getenv('DB_PORT', '5432'))
        self._db_name = os.getenv('DB_NAME')
        self._db_user = os.getenv('DB_USER')
        self._db_password = os.getenv('DB_PASSWORD')
        self._s3_rss_bucket = os.getenv('S3_RSS_BUCKET')

    def _load_from_secrets_manager(self):
        """Load configuration from AWS Secrets Manager (production mode)"""
        import boto3

        secrets_client = boto3.client('secretsmanager', region_name=self.region)

        try:
            response = secrets_client.get_secret_value(
                SecretId=os.getenv('SECRET_DATABASE_CONFIG', 'ai-stock-war/database-config')
            )
            self._config = json.loads(response['SecretString'])
        except Exception as e:
            raise RuntimeError(f"Failed to load config from Secrets Manager: {e}")

    @property
    def region(self) -> str:
        if self.dev_mode:
            return self._region
        return os.getenv('AWS_REGION', 'us-east-1')

    @property
    def db_host(self) -> str:
        if self.dev_mode:
            return self._db_host
        if not self._config:
            self._load_from_secrets_manager()
        return self._config['db_host']

    @property
    def db_port(self) -> int:
        if self.dev_mode:
            return self._db_port
        if not self._config:
            self._load_from_secrets_manager()
        return int(self._config['db_port'])

    @property
    def db_name(self) -> str:
        if self.dev_mode:
            return self._db_name
        if not self._config:
            self._load_from_secrets_manager()
        return self._config['db_name']

    @property
    def db_user(self) -> str:
        if self.dev_mode:
            return self._db_user
        if not self._config:
            self._load_from_secrets_manager()
        return self._config['db_user']

    @property
    def db_password(self) -> str:
        if self.dev_mode:
            return self._db_password
        if not self._config:
            self._load_from_secrets_manager()
        return self._config['db_password']

    @property
    def s3_rss_bucket(self) -> str:
        if self.dev_mode:
            return self._s3_rss_bucket
        if not self._config:
            self._load_from_secrets_manager()
        return self._config['s3_rss_bucket']


# Global settings instance
_settings: Optional[Settings] = None


def get_settings(dev_mode: bool = False) -> Settings:
    """Get global settings instance"""
    global _settings

    if _settings is None:
        env_dev_mode = os.getenv('DEV_MODE', 'false').lower() == 'true'
        _settings = Settings(dev_mode=dev_mode or env_dev_mode)

    return _settings


def reload_settings(dev_mode: bool = False):
    """Reload settings (force refresh)"""
    global _settings
    _settings = Settings(dev_mode=dev_mode)
