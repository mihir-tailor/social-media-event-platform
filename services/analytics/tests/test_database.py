"""
Unit tests for analytics database layer.
"""
import pytest
from unittest.mock import patch, MagicMock
from app.database import Database, AnalyticsRepository


@pytest.fixture
def mock_connection():
    """Mock MySQL connection."""
    with patch('app.database.Database.get_connection') as mock:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = None
        conn.cursor.return_value = cursor
        mock.return_value.__enter__.return_value = conn
        yield mock


class TestDatabase:
    """Tests for Database class."""
    
    def test_execute_query(self, mock_connection):
        """Test executing a query."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "test"}
        ]
        
        result = Database.execute_query("SELECT * FROM test")
        
        assert len(result) == 1
        assert result[0]["id"] == 1
    
    def test_execute_query_with_params(self, mock_connection):
        """Test executing query with parameters."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = []
        
        result = Database.execute_query(
            "SELECT * FROM test WHERE id = %s",
            (1,)
        )
        
        assert isinstance(result, list)
    
    def test_execute_single(self, mock_connection):
        """Test executing query for single result."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [{"id": 1}]
        
        result = Database.execute_single("SELECT * FROM test WHERE id = 1")
        
        assert result is not None
        assert result["id"] == 1
    
    def test_execute_single_no_result(self, mock_connection):
        """Test execute_single returns None when no results."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = []
        
        result = Database.execute_single("SELECT * FROM test WHERE id = 999")
        
        assert result is None


class TestAnalyticsRepository:
    """Tests for AnalyticsRepository."""
    
    def test_get_event_metrics(self, mock_connection):
        """Test getting event metrics."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {
                "window_start": "2024-01-15 10:00:00",
                "window_end": "2024-01-15 10:05:00",
                "event_type": "like",
                "platform": "web",
                "event_count": 100,
                "unique_users": 50,
                "computed_at": "2024-01-15 10:05:30"
            }
        ]
        
        results = AnalyticsRepository.get_event_metrics(
            time_window="5min",
            hours_back=1
        )
        
        assert len(results) == 1
        assert results[0]["event_type"] == "like"
        assert results[0]["event_count"] == 100
    
    def test_get_event_metrics_with_filters(self, mock_connection):
        """Test getting event metrics with filters."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = []
        
        results = AnalyticsRepository.get_event_metrics(
            time_window="5min",
            hours_back=24,
            event_type="like",
            platform="web",
            limit=50
        )
        
        assert isinstance(results, list)
    
    def test_get_user_metrics(self, mock_connection):
        """Test getting user metrics."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {
                "window_start": "2024-01-15 10:00:00",
                "window_end": "2024-01-15 11:00:00",
                "user_id": "user_123",
                "platform": "web",
                "total_events": 45,
                "likes": 20,
                "comments": 5,
                "shares": 3,
                "views": 15,
                "clicks": 2,
                "computed_at": "2024-01-15 11:00:30"
            }
        ]
        
        results = AnalyticsRepository.get_user_metrics(hours_back=1)
        
        assert len(results) == 1
        assert results[0]["user_id"] == "user_123"
        assert results[0]["total_events"] == 45
    
    def test_get_top_users(self, mock_connection):
        """Test getting top users."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {"user_id": "user_1", "platform": "web", "total_events": 100},
            {"user_id": "user_2", "platform": "ios", "total_events": 80}
        ]
        
        results = AnalyticsRepository.get_top_users(
            hours_back=24,
            limit=10
        )
        
        assert len(results) == 2
        assert results[0]["total_events"] == 100
    
    def test_get_event_distribution(self, mock_connection):
        """Test getting event distribution."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {"event_type": "like", "total_count": 5000},
            {"event_type": "view", "total_count": 8000}
        ]
        
        results = AnalyticsRepository.get_event_distribution(hours_back=24)
        
        assert len(results) == 2
        assert results[0]["event_type"] == "like"
    
    def test_get_platform_metrics(self, mock_connection):
        """Test getting platform metrics."""
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {"platform": "web", "total_events": 10000, "unique_users": 5000},
            {"platform": "ios", "total_events": 8000, "unique_users": 4000}
        ]
        
        results = AnalyticsRepository.get_platform_metrics(hours_back=24)
        
        assert len(results) == 2
        assert results[0]["platform"] == "web"