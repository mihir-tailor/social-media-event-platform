"""
Database connection and query utilities.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from .config import settings

logger = logging.getLogger(__name__)


class Database:
    """MySQL database connection manager."""
    
    _pool: Optional[pooling.MySQLConnectionPool] = None
    
    @classmethod
    def initialize_pool(cls):
        """Initialize connection pool."""
        if cls._pool is None:
            try:
                cls._pool = pooling.MySQLConnectionPool(
                    pool_name="analytics_pool",
                    pool_size=settings.DB_POOL_SIZE,
                    host=settings.MYSQL_HOST,
                    port=settings.MYSQL_PORT,
                    database=settings.MYSQL_DATABASE,
                    user=settings.MYSQL_USER,
                    password=settings.MYSQL_PASSWORD,
                    autocommit=True
                )
                logger.info("Database connection pool initialized")
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        """Get a connection from the pool."""
        if cls._pool is None:
            cls.initialize_pool()
        
        conn = cls._pool.get_connection()
        try:
            yield conn
        finally:
            conn.close()
    
    @classmethod
    def execute_query(cls, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
        
        Returns:
            List of dictionaries containing results
        """
        with cls.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(query, params or ())
                results = cursor.fetchall()
                return results
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                raise
            finally:
                cursor.close()
    
    @classmethod
    def execute_single(cls, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """Execute query and return single result."""
        results = cls.execute_query(query, params)
        return results[0] if results else None
    
    @classmethod
    def test_connection(cls) -> bool:
        """Test database connection."""
        try:
            with cls.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


class AnalyticsRepository:
    """Repository for analytics queries."""
    
    @staticmethod
    def get_event_metrics(
        time_window: str = "5min",
        hours_back: int = 1,
        event_type: Optional[str] = None,
        platform: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get event metrics for specified time window."""
        
        # Determine table based on time window
        table_map = {
            "5min": "event_metrics_5min",
            "1hr": "event_metrics_1hr",
            "1day": "event_metrics_1day"
        }
        table = table_map.get(time_window, "event_metrics_5min")
        
        # Build query
        query = f"""
            SELECT 
                window_start, window_end, event_type, platform,
                event_count, unique_users, computed_at
            FROM {table}
            WHERE window_start >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        """
        
        params = [hours_back]
        
        if event_type:
            query += " AND event_type = %s"
            params.append(event_type)
        
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        query += " ORDER BY window_start DESC LIMIT %s"
        params.append(limit)
        
        return Database.execute_query(query, tuple(params))
    
    @staticmethod
    def get_user_metrics(
        hours_back: int = 1,
        user_id: Optional[str] = None,
        platform: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get user activity metrics."""
        
        query = """
            SELECT 
                window_start, window_end, user_id, platform,
                total_events, likes, comments, shares, views, clicks,
                computed_at
            FROM user_metrics_1hr
            WHERE window_start >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        """
        
        params = [hours_back]
        
        if user_id:
            query += " AND user_id = %s"
            params.append(user_id)
        
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        query += " ORDER BY total_events DESC LIMIT %s"
        params.append(limit)
        
        return Database.execute_query(query, tuple(params))
    
    @staticmethod
    def get_top_users(hours_back: int = 24, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top users by activity."""
        
        query = """
            SELECT 
                user_id,
                platform,
                SUM(total_events) as total_events
            FROM user_metrics_1hr
            WHERE window_start >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            GROUP BY user_id, platform
            ORDER BY total_events DESC
            LIMIT %s
        """
        
        return Database.execute_query(query, (hours_back, limit))
    
    @staticmethod
    def get_event_distribution(hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get event type distribution."""
        
        query = """
            SELECT 
                event_type,
                SUM(event_count) as total_count
            FROM event_metrics_5min
            WHERE window_start >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            GROUP BY event_type
            ORDER BY total_count DESC
        """
        
        return Database.execute_query(query, (hours_back,))
    
    @staticmethod
    def get_platform_metrics(hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get platform-wise metrics."""
        
        query = """
            SELECT 
                platform,
                SUM(event_count) as total_events,
                SUM(unique_users) as unique_users
            FROM event_metrics_5min
            WHERE window_start >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            GROUP BY platform
            ORDER BY total_events DESC
        """
        
        return Database.execute_query(query, (hours_back,))