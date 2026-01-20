"""
Redis Client
Manage Redis connections and read real-time stock prices
"""

import redis
from typing import Optional, Dict, List
import ssl


class RedisClient:
    """Redis client (supports ElastiCache SSL)"""
    
    def __init__(
        self,
        host: str,
        port: int = 6379,
        db: int = 0,
        use_ssl: bool = True,
        decode_responses: bool = True,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5
    ):
        """
        Initialize the Redis client
        
        Args:
            host: Redis host
            port: Redis port
            db: database index
            use_ssl: whether to use SSL
            decode_responses: whether to auto-decode responses (strings)
            socket_timeout: socket timeout
            socket_connect_timeout: connection timeout
        """
        self.host = host
        self.port = port
        self.db = db
        
        # Configure connection parameters
        connection_kwargs = {
            'host': host,
            'port': port,
            'db': db,
            'decode_responses': decode_responses,
            'socket_timeout': socket_timeout,
            'socket_connect_timeout': socket_connect_timeout,
        }
        
        # Configure SSL (for ElastiCache)
        if use_ssl:
            connection_kwargs['ssl'] = True
            connection_kwargs['ssl_cert_reqs'] = ssl.CERT_NONE  # Do not validate certificate
        
        # Create Redis connection
        self.client = redis.StrictRedis(**connection_kwargs)
    
    def ping(self) -> bool:
        """
        Test connection
        
        Returns:
            True if connection is healthy
        """
        try:
            return self.client.ping()
        except Exception:
            return False
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value by key
        
        Args:
            key: key name
            
        Returns:
            Value, or None if missing
        """
        return self.client.get(key)
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """
        Set key value
        
        Args:
            key: key name
            value: key value
            ex: expiration in seconds
            
        Returns:
            True if set succeeds
        """
        return self.client.set(key, value, ex=ex)
    
    def delete(self, *keys: str) -> int:
        """
        Delete keys
        
        Args:
            keys: list of key names
            
        Returns:
            Number of deleted keys
        """
        return self.client.delete(*keys)
    
    def keys(self, pattern: str) -> List[str]:
        """
        Find keys matching pattern
        
        Args:
            pattern: key pattern (wildcards supported)
            
        Returns:
            List of matching keys
        """
        return self.client.keys(pattern)
    
    def get_stock_price(self, symbol: str) -> Optional[float]:
        """
        Get real-time stock price
        
        Args:
            symbol: stock symbol (e.g., NVDA)
            
        Returns:
            Stock price, or None if missing
        """
        key = f"stock:price:{symbol}"
        price_str = self.get(key)
        
        if price_str is None:
            return None
        
        try:
            return float(price_str)
        except (ValueError, TypeError):
            return None
    
    def get_all_stock_prices(self) -> Dict[str, float]:
        """
        Get all stock prices
        
        Returns:
            {symbol: price} dictionary
        """
        keys = self.keys("stock:price:*")
        prices = {}
        
        for key in keys:
            # Extract "NVDA" from "stock:price:NVDA"
            symbol = key.split(':')[-1]
            price = self.get_stock_price(symbol)
            
            if price is not None:
                prices[symbol] = price
        
        return prices
    
    def set_stock_price(self, symbol: str, price: float, ex: int = 3600) -> bool:
        """
        Set stock price (for testing or data updates)
        
        Args:
            symbol: stock symbol
            price: stock price
            ex: expiration in seconds (default 1 hour)
            
        Returns:
            True if set succeeds
        """
        key = f"stock:price:{symbol}"
        return self.set(key, str(price), ex=ex)
    
    def close(self):
        """Close connection"""
        if self.client:
            self.client.close()
    
    def __del__(self):
        """Destructor: auto-close connection"""
        self.close()


# Global singleton (optional)
_redis_client_instance: Optional[RedisClient] = None


def get_redis_client(
    host: str,
    port: int = 6379,
    use_ssl: bool = True
) -> RedisClient:
    """
    Get global RedisClient singleton
    
    Args:
        host: Redis host
        port: Redis port
        use_ssl: whether to use SSL
        
    Returns:
        RedisClient instance
    """
    global _redis_client_instance
    
    if _redis_client_instance is None:
        _redis_client_instance = RedisClient(
            host=host,
            port=port,
            use_ssl=use_ssl
        )
    
    return _redis_client_instance
