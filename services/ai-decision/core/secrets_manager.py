"""
AWS Secrets Manager Client
Load configs from AWS Secrets Manager with caching and auto refresh
"""

import json
import boto3
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


class SecretsManager:
    """AWS Secrets Manager client"""
    
    def __init__(self, region: str = 'us-east-1', cache_ttl: int = 300):
        """
        Initialize the Secrets Manager client
        
        Args:
            region: AWS region
            cache_ttl: cache TTL in seconds (default 5 minutes)
        """
        self.region = region
        self.cache_ttl = cache_ttl
        self.client = boto3.client('secretsmanager', region_name=region)
        
        # Cache: {secret_name: {'value': dict, 'expires_at': datetime}}
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def get_secret(self, secret_name: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Retrieve a secret value (with caching)
        
        Args:
            secret_name: secret name
            force_refresh: whether to force cache refresh
            
        Returns:
            Secret content (JSON-decoded dictionary)
            
        Raises:
            ClientError: AWS API call failed
            json.JSONDecodeError: Secret content is not valid JSON
        """
        # Check cache
        if not force_refresh and secret_name in self._cache:
            cached = self._cache[secret_name]
            if datetime.now() < cached['expires_at']:
                return cached['value']
        
        # Read from Secrets Manager
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            secret_string = response['SecretString']
            secret_value = json.loads(secret_string)
            
            # Update cache
            self._cache[secret_name] = {
                'value': secret_value,
                'expires_at': datetime.now() + timedelta(seconds=self.cache_ttl)
            }
            
            return secret_value
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                raise ValueError(f"Secret not found: {secret_name}")
            elif error_code == 'InvalidRequestException':
                raise ValueError(f"Invalid request for secret: {secret_name}")
            elif error_code == 'InvalidParameterException':
                raise ValueError(f"Invalid parameter for secret: {secret_name}")
            else:
                raise RuntimeError(f"Failed to retrieve secret {secret_name}: {e}")
        
        except json.JSONDecodeError as e:
            raise ValueError(f"Secret {secret_name} is not valid JSON: {e}")
    
    def clear_cache(self, secret_name: Optional[str] = None):
        """
        Clear cache
        
        Args:
            secret_name: name to clear; None clears all
        """
        if secret_name:
            self._cache.pop(secret_name, None)
        else:
            self._cache.clear()
    
    def is_cached(self, secret_name: str) -> bool:
        """
        Check if a secret is cached and not expired
        
        Args:
            secret_name: secret name
            
        Returns:
            True if cached and valid
        """
        if secret_name not in self._cache:
            return False
        
        return datetime.now() < self._cache[secret_name]['expires_at']


# Global singleton (optional)
_secrets_manager_instance: Optional[SecretsManager] = None


def get_secrets_manager(region: str = 'us-east-1') -> SecretsManager:
    """
    Get the global SecretsManager singleton
    
    Args:
        region: AWS region
        
    Returns:
        SecretsManager instance
    """
    global _secrets_manager_instance
    
    if _secrets_manager_instance is None:
        _secrets_manager_instance = SecretsManager(region=region)
    
    return _secrets_manager_instance
