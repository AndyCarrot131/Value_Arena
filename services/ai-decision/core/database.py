"""
PostgreSQL Database Manager
Manage database connection pool, transactions, and queries
"""

import psycopg2
from psycopg2 import pool, extras
from psycopg2.extensions import connection, cursor
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import time


class DatabaseManager:
    """PostgreSQL database manager"""
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        minconn: int = 2,
        maxconn: int = 10
    ):
        """
        Initialize the database connection pool
        
        Args:
            host: database host
            port: database port
            database: database name
            user: database user
            password: database password
            minconn: minimum connections
            maxconn: maximum connections
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        
        # Create connection pool
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )
    
    def get_connection(self) -> connection:
        """
        Get a connection from the pool
        
        Returns:
            Database connection object
            
        Raises:
            pool.PoolError: pool exhausted
        """
        return self.pool.getconn()
    
    def release_connection(self, conn: connection):
        """
        Return a connection to the pool
        
        Args:
            conn: database connection object
        """
        self.pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, commit: bool = False):
        """
        Context manager: automatically acquire and release connection
        
        Args:
            commit: whether to auto-commit
            
        Yields:
            Database cursor
            
        Example:
            with db.get_cursor(commit=True) as cur:
                cur.execute("INSERT INTO ...")
        """
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        
        try:
            yield cur
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            self.release_connection(conn)
    
    @contextmanager
    def transaction(self):
        """
        Context manager: atomic transaction
        
        Yields:
            Database cursor
            
        Example:
            with db.transaction() as cur:
                cur.execute("UPDATE wallets ...")
                cur.execute("INSERT INTO transactions ...")
                # Auto commit or rollback
        """
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            self.release_connection(conn)
    
    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        fetch: bool = True
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a query (with automatic retries)
        
        Args:
            query: SQL query string
            params: query parameters
            fetch: whether to fetch results
            
        Returns:
            Query results (list of dicts); returns None if fetch=False
            
        Raises:
            Exception: query failed
        """
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                with self.get_cursor() as cur:
                    cur.execute(query, params)
                    
                    if fetch:
                        results = cur.fetchall()
                        # Convert to list of dicts
                        return [dict(row) for row in results]
                    else:
                        return None
                        
            except psycopg2.OperationalError as e:
                # Network error, retry
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
                else:
                    raise RuntimeError(f"Database query failed after {max_retries} retries: {e}")
            
            except Exception as e:
                # Other errors, do not retry
                raise RuntimeError(f"Database query failed: {e}")
    
    def execute_update(
        self,
        query: str,
        params: Optional[Tuple] = None
    ) -> int:
        """
        Execute update operations (INSERT/UPDATE/DELETE)
        
        Args:
            query: SQL update statement
            params: query parameters
            
        Returns:
            Number of affected rows
            
        Raises:
            Exception: update failed
        """
        with self.get_cursor(commit=True) as cur:
            cur.execute(query, params)
            return cur.rowcount
    
    def execute_many(
        self,
        query: str,
        params_list: List[Tuple]
    ) -> int:
        """
        Execute batch operations
        
        Args:
            query: SQL statement
            params_list: list of parameter tuples
            
        Returns:
            Total affected rows
            
        Raises:
            Exception: execution failed
        """
        with self.get_cursor(commit=True) as cur:
            extras.execute_batch(cur, query, params_list)
            return cur.rowcount
    
    def close(self):
        """Close the connection pool"""
        if self.pool:
            self.pool.closeall()
    
    def __del__(self):
        """Destructor: automatically close connection pool"""
        try:
            self.close()
        except:
            pass  # Ignore errors during cleanup


# Global singleton (optional)
_database_manager_instance: Optional[DatabaseManager] = None


def get_database_manager(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str
) -> DatabaseManager:
    """
    Get the global DatabaseManager singleton
    
    Args:
        host: database host
        port: database port
        database: database name
        user: database user
        password: database password
        
    Returns:
        DatabaseManager instance
    """
    global _database_manager_instance
    
    if _database_manager_instance is None:
        _database_manager_instance = DatabaseManager(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    
    return _database_manager_instance
